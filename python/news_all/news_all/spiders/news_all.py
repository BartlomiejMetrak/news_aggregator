from datetime import datetime, timedelta
import feedparser
import logging
import scrapy

from .additional_code_rss_feed import try_parse_date, try_parse_summary, try_parse, try_parse_link
from .additional_code_article import article_extraction, hashing_SHA2, get_hash_keys, get_page_content, check_article_box
from ..mysql_db import table_management
from ..items import NewsAllItem
from ..settings import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_aalerts_backend = config['table_aalerts_backend']
table_sources_news_rss = config['table_sources_news_rss']


''' 
Skrypt pobiera linki rss_feed z podanych w tabeli w bd i następnie pobiera listę artykułów - tytuł, datę, opis itp.
po pobraniu danych z RSS_Feed sprawdzany jest hash_article (link, tytuł i data) i odfiltrowywane są newsy zapisane już w bazie

Nastepnie, pozostały tylko nowe newsy. Skrypt wchodzi na podstrony każdego z newsów i pobiera treść artykułu oraz obrazek.
Potem artykuły są zapisywane do bazy.

Do zrobienia:
1. przemyśleć zapisywanie liczby zablokowanych odpytań do tej samej strony/gazety i jeśli ta liczba będzie duża w danym czasie 
to wysyłać alert o podejrzeniu zablokowania IP itp.
'''

date_past = datetime.now().date() - timedelta(days=2)


# Create Spider class
class News_all_rss_feed(scrapy.Spider):


    name = 'news_all'
    hash_keys_in_db = get_hash_keys(date_past)


    def start_requests(self):
        cls = table_management(hostname, dbname, uname, pwd)
        df_sources = cls.get_multi_filtered_columns_df(table_sources_news_rss, '*', 'block_rss = 0')  #pobieranie linków do rss_feed - pomijamy blokowane rss
        cls.close_connection_2()

        #df_sources = df_sources[df_sources['source'].isin(['forsal', 'wojciech bialek'])] #['strefa inwestorów', 'wnp', 'fxmag'])]
        sources_dictionary_lst = df_sources.to_dict(orient='records')

        rss_feed_requests = []
        for row in sources_dictionary_lst:
            url = row['rss_link']
            rss_feed_requests.append(scrapy.Request(url=url, meta={'dict_sources': row}, callback=self.parse, errback=self.errback_func))
        return rss_feed_requests

    def parse_feed(self, feed):
        """
        Parse RSS/Atom feed using feedparser
        """
        data = feedparser.parse(feed)
        if data.bozo:
            logging.error('Bozo feed data. %s: %r',
                    data.bozo_exception.__class__.__name__,
                    data.bozo_exception)
            if (hasattr(data.bozo_exception, 'getLineNumber') and
                    hasattr(data.bozo_exception, 'getMessage')):
                line = data.bozo_exception.getLineNumber()
                logging.error('Line %d: %s', line, data.bozo_exception.getMessage())
                segment = feed.split('\n')[line-1]
                logging.info('Body segment with error: %r', segment)
            # could still try to return data. not necessarily completely broken
            return None
        return data

    def parse(self, response, **kwargs):
        # parse downloaded content with feedparser (NOT re-downloading with feedparser)
        feed = self.parse_feed(response.body)
        if feed:
            # counter = 0

            for entry in feed.entries:
                dict_sources = response.meta['dict_sources']

                article_download = dict_sources['art_download']
                image_download = dict_sources['img_download']

                items = NewsAllItem()
                items.set_all()  # dajemy wartość none dla każdej wartości na początku

                items['source'] = dict_sources['source']
                items['source_id'] = dict_sources['id']
                items['link'] = try_parse_link(entry, 'link')
                items['title'] = try_parse(entry, 'title')
                items['description'] = try_parse_summary(entry, 'summary', dict_sources['id'])

                if dict_sources['id'] == 4:  # jeśli bankier.pl to ręcznie cofamy o 1h > bo mają złą strefę czasową
                    parsed_date = try_parse_date(entry, 'published')
                    items['date'] = parsed_date - timedelta(hours=1)
                else:
                    items['date'] = try_parse_date(entry, 'published')

                string_to_hash = str(items['link']) + str(items['title']) + str(items['date'])
                hash_article = hashing_SHA2(string_to_hash)  #unikalny hash dla każdego artykułu
                items['hash_article'] = hash_article

                if hash_article not in self.hash_keys_in_db and items['date'].date() >= date_past:  #jeśli tego artykułu nie ma jeszcze w bazie
                    if bool(items['link']):  # jeśli jest link do artykułu
                        yield scrapy.Request(url=items['link'], callback=self.parse_article, errback=self.errback_article_scrapping,
                                             meta={'items': items, 'article_download': article_download, 'image_download': image_download})
                    # counter += 1
                    # if counter == 2:
                    #     break

    def get_article_content(self, content, article_download, image_download, source_id):
        """
        Parse article using scrapy
        """
        article = article_extraction(article_link=content, article_download=article_download, image_download=image_download)
        dict_article = article.parse_article()

        comparison = check_article_box(response=content, extracted_article=dict_article['article'],
                                       extracted_image=dict_article['image'], source_id=source_id)
        extraction_score, words_in_art_perc, article_cleaned = comparison.compare_texts()
        image_url = comparison.compare_images()

        dict_article['article'] = article_cleaned
        dict_article['image'] = image_url
        dict_article['extraction_score'] = extraction_score
        dict_article['words_in_art_perc'] = words_in_art_perc
        return dict_article

    def parse_article(self, response):
        # pobieranie artykułu bez ponownego scrapowania strony - przekazywany jest response.body to newspaper3k
        items = response.meta['items']
        source_id = items['source_id']

        article_download = response.meta['article_download']
        image_download = response.meta['image_download']

        dict_article = self.get_article_content(response.body, article_download, image_download, source_id)

        items['article'] = dict_article['article']
        items['image'] = dict_article['image']
        items['extraction_score'] = dict_article['extraction_score']
        items['words_in_art_perc'] = dict_article['words_in_art_perc']
        yield items

    def errback_func(self, failure):
        """ funkcja do zarządzania błędem pobieranie feedu z rss_feed """
        date = datetime.now()
        request = failure.request

        info = f'błąd przy pobieraniu newsów z rss_feed, url: {request.url} dnia {date}'
        cls = table_management(hostname, dbname, uname, pwd)
        cls.add_data_row(table_aalerts_backend, [info, date, 'news_all'], '(info,updated,table_name)', '(%s, %s, %s)')
        cls.close_connection_2()

    def errback_article_scrapping(selfself, failure):
        """
        funkcja do mierzenia ilości zablokowanych wejść na stroneartykułu, aby pobrać jego treść
        za każdym razem gdy wystąpi błąd to pobijana jest dzienna liczba błędnych pobrań
        w przypadku wystapienia maksymalnej wartości dodawanych jest alert do tabeli w bazie
        """
        response = failure.value.response
        items = response.meta['items']

        date = datetime.now()
        sourceID = items['source_id']

        cls = table_management(hostname, dbname, uname, pwd)
        err_daily, err_max, date_db = cls.fetch_one_result_filtered(table_sources_news_rss, 'article_error_daily,article_error_max,updated_at', f'id = {sourceID}')

        if date_db.day == date.day:  #jeśli dalej ten sam dzień
            err_daily = err_daily + 1
            if err_daily > err_max:  # jeśli dzienna liczba błędów przekorczy historyczną wartość
                info_max = f'Zostałą przekroczona maksymalna historyczna wartość błędów przy scrapowaniu strony ({err_daily}) - możliwe blokowanie IP lub zbyt częste odpytania. Source: {items["source"]}, url: {items["link"]}'
                cls.add_data_row(table_aalerts_backend, [info_max, date, 'news_article'], '(info,updated,table_name)', '(%s, %s, %s)')
                cls.update_value(table_sources_news_rss, 'article_error_max', err_daily, 'id', sourceID)  #nowy historyczny rekord
            cls.update_value(table_sources_news_rss, 'article_error_daily', err_daily, 'id', sourceID)  #nowa dzienna wartość
        else:
            cls.update_value(table_sources_news_rss, 'article_error_daily', 1, 'id', sourceID)  #nan owy dzień wartośc równa 1
            cls.update_value(table_sources_news_rss, 'updated_at', date, 'id', sourceID)  # nan owy dzień wartośc równa 1
        cls.close_connection_2()
