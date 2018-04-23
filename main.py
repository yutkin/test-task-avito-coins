import asyncio
import aiohttp
import argparse
from requests_html import HTML
import uvloop
import logging, coloredlogs
import random
import re
from fake_useragent import UserAgent
from natasha import DatesExtractor
from datetime import datetime
import shelve

class AvitoParser:
    def __init__(self, debug=False, off_sleeps=False):
        """
        Класс асинхронного парсера объявлений с Avito, раздел "коллекционирование монет"
        :param debug: Включить debug сообщения
        :param off_sleeps: Использовать слипы между запросами (для обхода каптч)
        """
        self._off_sleeps = off_sleeps
        self._loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.set_debug(debug)

        self._queue = asyncio.Queue(loop=self._loop)
        self._base_url = 'https://www.avito.ru/moskva/kollektsionirovanie/monety?view=list&p={}'
        self._run_loop = True

        self._log = logging.getLogger(self.__class__.__name__)
        coloredlogs.install(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.DEBUG if debug else logging.INFO)

        self._sess = None
        self._user_agent = UserAgent()

        self._db = shelve.open('./parser.db')

        self._n_ads = 0
        self._n_ads_have_year = 0
        self._n_rsfsr_ads = 0
        self._n_rus_empire_ads = 0
        self._dates_extractor = DatesExtractor()

    async def _random_sleep(self, lo=3, hi=5, log=True):
        """
        Чтобы не словить каптчу, имитируем случайные паузы между запросами
        :param lo: Нижняя граница для генерации секунд
        :param hi: Верхняя граница для генерации секунд
        :param log: Логировать sleeps
        """
        sleep_s = random.randint(lo, hi)
        if sleep_s:
            if log:
                self._log.debug('Falling asleep for %d seconds...' % sleep_s)
            await asyncio.sleep(sleep_s)

    def _produced_by_rus_empire(self, text, year):
        """
        Возвращает True, если монета сделана в Российской Империи, иначе False
        :param text: Текст объявления
        :param year: Год выпуска монеты
        """
        text = text.lower().strip()
        year = year or 0

        if ('россий' in text and 'импер' in text) or \
           ('russian' in text and 'empire' in text):
            return True

        keywords = ('россий', 'russian', 'рубл', 'копейк', 'копеек', 'гривен', 'червон', )
        if any([k in text for k in keywords]) and 1721 <= year <= 1917:
            return True

        return False

    def _produced_by_rsfsr(self, text, year):
        """
        Возвращает True, если монета сделана в РСФСР, иначе False
        --
        Прим. все монеты сделанные в СССР, также сделаны в РСФСР, т.к. в других республиках
        еще не было монетных дворов. Пруф: https://ru.wikipedia.org/wiki/Государственный_банк_СССР
        --
        :param text: Текст объявления
        :param year: Год выпуска монеты
        """
        text = text.lower().strip()
        year = year or 0

        keywords1 = ('ссср', 'рсфср', 'советск', )
        keywords2 = ('рубл', 'копейк', 'копеек', )

        return ((year == 0 or 1922 <= year <= 1991) and any([k in text for k in keywords1])) or \
               (1922 <= year <= 1991 and any([k in text for k in keywords2]))

    def _get_year_from_text(self, text):
        """
        Получает год выпуска из текста объявления
        :param text: Текст объявления
        :return: Год если в тексте есть год, иначе None
        """
        regexp = r'[^a-bA-B0-9](?P<num>\d{4})[^a-zA-Z0-9]'
        found = re.search(regexp, text)
        if found:
            num = found.groupdict().get('num')
            if num and (1000 <= int(num) <= datetime.now().year):
                return int(num)

        years = [x.fact.year for x in self._dates_extractor(text)
                 if x.fact.year is not None]
        if years:
            return years[0]
        else:
            return None

    async def _get_title_text(self, ads_url):
        """
        По урлу объявления получает его заголовок и текст описания
        :param ads_url: URL объявления
        :return: Заголовок и текст объявления. Если ошибка или не найдено, то (None, None)
        """
        headers = {'User-Agent': self._user_agent.random}
        async with self._sess.get(ads_url, headers=headers) as resp:
            if resp.status != 200:
                return None, None

            html = HTML(html=await resp.text())

            title = html.find('.title-info-title-text', first=True)
            if title is not None:
                title = title.text.lower()

            text = html.find('div[itemprop=description]', first=True)
            if text is not None:
                text = text.text.lower()

            return title, text

    async def _consume(self):
        """
        Асинхронный консьюмер. Достает урлы из очереди и обрабатывает
        """
        while True:
            url = await self._queue.get()

            self._log.info('Processing {} [Done: {} | In queue: {}]'.format(
                url, self._n_ads, self._queue.qsize()))

            cached = False
            if url not in self._db:
                title, text = await self._get_title_text(url)
                self._db[url] = (title, text)
            else:
                title, text = self._db[url]
                cached = True

            if title is None or text is None:
                self._log.debug('Title or Text is None {}'.format(url))
            ads_text = '{} {}'.format(title, text)


            year = self._get_year_from_text(ads_text)
            self._n_ads_have_year += int(year is not None)

            if year is None:
                self._log.debug('{} has not year'.format(url))

            if self._produced_by_rsfsr(ads_text, year):
                self._n_rsfsr_ads += 1
                self._log.info('{} [{}] produced in RSFSR'.format(url, title))

            if self._produced_by_rus_empire(ads_text, year):
                self._n_rus_empire_ads += 1
                self._log.info('{} [{}] produced in Russian Empire'.format(url, title))

            self._queue.task_done()
            self._n_ads += 1

            if not self._off_sleeps and not cached:
                await self._random_sleep()

    async def _fetch_page(self, page_num):
        """
        Асинхронно получает страницу со списком объявлений
        :param page_num: Pagination. Номер страницы
        :return: 1, если страница успешно получена, иначе 0
        """
        url = self._base_url.format(page_num)
        headers = {'User-Agent': self._user_agent.random}

        self._log.info('Fetching %s' % url)

        cached = False
        html = None
        if url in self._db:
            html = self._db[url]
            cached = True
        else:
            async with self._sess.get(url, allow_redirects=False, headers=headers) as resp:
                if resp.status == 200:
                    html = await resp.text()
                elif resp.status == 302: # Redirect
                    if 'blocked' in resp.headers.get('Location'):
                        if not self._off_sleeps:
                            sleep_s = random.randint(15, 30)
                            self._log.error('Got captcha! Falling asleep for %d seconds...' % sleep_s)
                            await asyncio.sleep(sleep_s)
                        else:
                            self._log.error('Got captcha!')
                    else:
                        self._log.warning('%s does not exist. Stopping the loop...' % url)
                        self._run_loop = False

        if html is not None:
            self._db[url] = html
            parsed_html = HTML(html=html).find('.description-title-link')
            for el in parsed_html:
                await self._queue.put('https://avito.ru/' + el.attrs['id'])
            self._queue.task_done()

        if not self._off_sleeps and not cached:
            await self._random_sleep()

        return int(html is not None)

    def run_parsing(self, page_num=1):
        """
        Запускает основной цикл асинхронного парсера Avito
        :param page_num: С какой страницы начинать парсинг
        """
        consumer = asyncio.ensure_future(self._consume(), loop=self._loop)

        cur_page = page_num
        while self._run_loop:
            cur_page += self._loop.run_until_complete(self._fetch_page(cur_page))

        self._loop.run_until_complete(self._queue.join())
        consumer.cancel()

    @property
    def total_ads(self):
        return self._n_ads

    @property
    def ads_without_year(self):
        return self._n_ads - self._n_ads_have_year

    @property
    def rsfsr_ads(self):
        return self._n_rsfsr_ads

    @property
    def russian_empire_ads(self):
        return self._n_rus_empire_ads

    def __enter__(self):
        self._sess = aiohttp.ClientSession(loop=self._loop)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ret = exc_type is None
        if exc_type is KeyboardInterrupt:
            ret = True
            self._log.info('Stopping...')
        self._loop.run_until_complete(self._sess.close())
        self._loop.stop()
        self._db.close()
        return ret


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-d', '--debug', help='Run in debug mode', action='store_true')
    argparser.add_argument('--off-sleeps',
                           help='Do not sleep after requests. (You will get a captcha !!!)',
                           action='store_true')
    args = argparser.parse_args()

    with AvitoParser(debug=args.debug, off_sleeps=args.off_sleeps) as parser:
        parser.run_parsing(page_num=1)

    print('Монеты:')
    print('* Всего:', parser._n_ads)
    print('* Без указания года выпуска:', parser.ads_without_year)
    print('* РСФСР:', parser.rsfsr_ads)
    print('* Российской Империи:', parser.russian_empire_ads)
    print('* Соотношение монет РСФСР к монетам Российской Империи:',
          parser.rsfsr_ads / (parser.russian_empire_ads + 1e-9))

if __name__ == '__main__':
    main()
