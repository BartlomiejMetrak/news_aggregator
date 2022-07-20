from scrapy.exceptions import DropItem
from .mysql_db import table_management
import mysql.connector
from .settings import config

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']



class NewsAllPipeline:
    def process_item(self, item, spider):

        hash_article = item['hash_article']

        if hash_article is None:
            raise DropItem("Brak danych dla tego artykułu" % item)
        if item['source_id'] == 15 and '– spojrzenie na wykres' in item['title']:  #dla Stockwatch - wpisy premium bez summary są >> odrzucamy
            if item['description'] is None or len(item['description']) <= 1:
                raise DropItem("Brak danych dla tego artykułu" % item)

        dict_data = {'sourceID': item['source_id'],
                     'source': item['source'],
                     'date': item['date'],
                     'title': item['title'],
                     'summary': item['description'],
                     'article': item['article'],
                     'link': item['link'],
                     'hash_article': item['hash_article'],
                     'image': item['image'],
                     'extraction_score': item['extraction_score'],
                     'words_in_art_perc': item['words_in_art_perc'],
                     'new': 1,
                     }
        dict_data = {k: None if not v else v for k, v in dict_data.items()}

        #self.update_data_table(dict_data)
        try:
            self.update_data_table(dict_data)
        except mysql.connector.IntegrityError:
            print(">>>> Alert! Podany artykuł już istnieje w bazie.")
        return item

    def update_data_table(self, dict_data):
        cls = table_management(hostname, dbname, uname, pwd)
        col_names = list(dict_data.keys())
        col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
        values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"
        data = list(dict_data.values())
        cls.add_data_row('news_all', data, col_names_string, values_string)  # dodać index
        cls.close_connection_2()
