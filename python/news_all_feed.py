from scrapy.crawler import CrawlerProcess
from news_all.news_all.spiders.news_all import News_all_rss_feed

from mysql_db import table_management
from config import config

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_all = config['table_news_all']  #wszystkie pobrane newsy z gazet i portali w bazie
news_max_rows_table_news_all = int(config['news_max_rows_table_news_all'])



def delete_rows_if_needed():
    max_rows = news_max_rows_table_news_all  # max ilośc newsów w bd
    no_to_be_deleted = max_rows * 0.1  # usuwamy 10% z tego, jeśli zostanie przekroczona granica

    cls = table_management(hostname, dbname, uname, pwd)
    ids_lst = cls.fetch_all_results(table_news_all, 'id')
    ids_lst = [x[0] for x in ids_lst]
    ids_lst.sort()  #posortowana lista idków od malejących do rosnących

    lgth = len(ids_lst)  # bierzemy łaczną liczbę newsów

    if lgth > max_rows:
        lowest_id = min(ids_lst)  # bierzemy najmniejszy ID z bazy danych

        x = lowest_id + no_to_be_deleted  # wszystkie ID poniżej tego zostaną usunięte
        cls.delete_rows_condition(table_news_all, f'id < {x}')
        print("\nwiersze od %s do %s zostały wykasowane" % (lowest_id, x))
        print("\nobecnie jest %s wierszy w tabeli" % int(lgth - no_to_be_deleted))
    cls.close_connection_2()



process = CrawlerProcess(settings={
    "ITEM_PIPELINES": {'news_all.news_all.pipelines.NewsAllPipeline': 300, },
    "ROBOTSTXT_OBEY": False,  #ale tylko dla biznes.pap nie można, dla reszty gazet można
    "USER_AGENT": 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
    "CONCURRENT_REQUESTS": 32,
    "DOWNLOAD_DELAY": 1,
    "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    "COOKIES_ENABLED": True,
    "COOKIES_DEBUG": True,
    #"LOG_ENABLED": False,  #nie wyświetlamy niczego ze scrapy
    "LOG_LEVEL": 'INFO',  #wyświetlanie tylko informacji o spider bot, bez pobranych danych
    "config": config,
})


def run_script():
    process.crawl(News_all_rss_feed)
    process.start()  # the script will block here until the crawling is finished

    delete_rows_if_needed()  # usuwanie wierszy jeśli tabela przekroczy maksymalny rozmiar

