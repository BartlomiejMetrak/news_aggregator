from mysql_db import table_management
from datetime import datetime, timedelta
from news_company_keywords_articles import get_articles
from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_keywords = config['table_news_companies_keywords']  #słowa kluczowe słów
table_companies = config['table_companies']
table_config_table = config['table_config_table']


# zmienne
news_shorter_timeframe = int(config['news_shorter_timeframe'])
news_longer_timeframe = int(config['news_shorter_timeframe'])



'''
skrypt odpala skrypt do pobierania przykładowych artykułów o spółkach z bankier.pl
następnie skrypt analizuje nowe spółki i wyszukuje ich wariancje nazwy i zapisuje w bd
'''

"""
Schemat działania zedytowanego skryptu:

1. bierzemy tylko aktywne spółki i dla nich keywords set z tabeli news_companies_keyword
2. na giełdę wchodzi nowa spółka - w tabeli jest jako new_comp - do momentu, aż recznie jej nie zaakceptujemy to nie będzie to aktywny keyword (dać alert)
3. po akceptacji dostepny jest search name
4. po 30 i 90 dniach od IPO przeszukiwane są newsy w bazie (na podstawie search_name) i dodawwane są 
    nowe formy odmiam keywordsów z wartością new_comp = 1 i alertem do bazy w celu ręcznej akceptacji
5. dodać kolumny updated_at i do aktualizacji
"""


date = datetime.now()

shorter_timeframe = news_shorter_timeframe
longer_timeframe = news_longer_timeframe


class update_keywords:

    def __init__(self):
        cls_0 = table_management(hostname, dbname, uname, pwd)
        self.active_comp_table = cls_0.get_multi_filtered_columns_df(table_companies, 'id,emitent,name_search', 'active = 1')  # spółki aktywne na gpw
        self.keywords_table = cls_0.get_columns_data(table_news_companies_keywords, 'compID_f,keyword,to_be_updated,updated_at')  # spółki z keywordsami w bazie
        cls_0.close_connection_2()

    def get_shorter_time_update(self):
        """
        funkcja do aktualizowania tabeli z keywordsami z krótszego okresu - wszystkie spółki, które były updated_at
        dłużej niż x dni i mają to_be_updated = 1 zostają wliczane
        """
        shorter_date = date - timedelta(days=shorter_timeframe)
        keywords_to_update = self.keywords_table[(self.keywords_table['updated_at'] < shorter_date) & (self.keywords_table['to_be_updated'] == 1)]  #aktualizujemy daty starsze niż 30 dni
        compID_list = keywords_to_update['compID_f'].unique().tolist()
        print(f"spółki do zaktualizowania w krótszym terminie: {compID_list}")

        cls_2 = table_management(hostname, dbname, uname, pwd)
        for compID in compID_list:
            cls_2.update_value(table_news_companies_keywords, "to_be_updated", 2, "compID_f", compID)  #najpierw updatujemy wartość żeby potem nie było że keywordsy z nową datą też będą updatowane
            articles = get_articles(compID)
            articles.new_words()
        cls_2.close_connection_2()

    def get_longer_time_update(self):
        """
        funkcja do aktualizowania tabeli z keywordsami z dłuższego okresu - wszystkie spółki, które były updated_at
        dłużej niż x dni i mają to_be_updated = 2 zostają wliczane
        po dodaniu keywordsów aktualizowana jest kolumna to_be_updated na 0 >> czyli już nie będzie aktualizowania odmian,
        do momentu ręcznego odpalenia
        """
        longer_date = date - timedelta(days=longer_timeframe)
        keywords_to_update = self.keywords_table[(self.keywords_table['updated_at'] < longer_date) & (self.keywords_table['to_be_updated'] == 2)]  #aktualizujemy daty starsze niż 30 dni
        compID_list = keywords_to_update['compID_f'].unique().tolist()
        print(f"spółki do zaktualizowania w dłuższym terminie: {compID_list}")

        cls_2 = table_management(hostname, dbname, uname, pwd)
        for compID in compID_list:
            articles = get_articles(compID)
            articles.new_words()
            cls_2.update_value(table_news_companies_keywords, "to_be_updated", 0, "compID_f", compID)
        cls_2.close_connection_2()

    def get_new_companies(self):
        """
        funkcja do sprawdzania nowych id aktywnych spółek i ewentualnego dodawania wierszy do news_companies_keywords
        z keyword = name_search oraz new i updated_at
        """
        news_comp_lst = self.keywords_table['compID_f'].unique().tolist()
        active_comps = self.active_comp_table['id'].unique().tolist()
        new_comps = list(set(active_comps) - set(news_comp_lst))  #nowe spółki do dodania do tabeli z keywordsami

        if new_comps is not None and len(new_comps) > 0:
            new_keywords = self.active_comp_table[self.active_comp_table['id'].isin(new_comps)]

            columns_df = list(new_keywords.columns)
            id = columns_df.index('id') + 1
            emitent = columns_df.index('emitent') + 1
            name_search = columns_df.index('name_search') + 1

            columns = ['compID_f', 'emitent', 'keyword', 'new_keyword', 'to_be_updated']  # kolumny w BD
            cls_2 = table_management(hostname, dbname, uname, pwd)  # table_name, col_name, val, col_condition, value_condition)
            col_names_string = "(" + ",".join([str(i) for i in columns]) + ")"
            values_string = "(" + ", ".join(["%s"] * len(columns)) + ")"

            print("Nowe keywordsy spółek do dodania do tabeli news_companies_keywords:")
            for ir in new_keywords.itertuples():
                dict = {
                    'compID_f': ir[id],
                    'emitent': ir[emitent],
                    'keyword': ir[name_search].lower().strip(),
                    'new_keyword': 0,  #nowe spółki z keywordsem z search_name od razu akceptujemy
                    'to_be_updated': 1,
                }
                print(dict)
                data = list(dict.values())
                cls_2.add_data_row(table_news_companies_keywords, data, col_names_string, values_string)
            cls_2.close_connection_2()



def updated_config(config):
    """
    funkcja do updatowanie config - bo keywords list aktualizujemy w wybrane dni raz dziennie, a cały skrypt będzie odpalany co kilka minut
    :return:
    """
    if config == 0:
        print("Zmiania config z 0 na 1 >> keywords list zostaną zaktualizowane jutro o wskazanej godzinie (raz).")
        cls_2 = table_management(hostname, dbname, uname, pwd)
        cls_2.update_value(table_config_table, "config", 1, "id", 5)
        cls_2.update_value(table_config_table, "updated_at", date, "id", 5)
        cls_2.close_connection_2()

def updated_config_2():
    """
    jeśli już skrypt do keywords list zostanie odpalony to na kocu config zamieniamy na 0 i dajemy date.now()
    :return:
    """
    cls_2 = table_management(hostname, dbname, uname, pwd)
    cls_2.update_value(table_config_table, "config", 0, "id", 5)
    cls_2.update_value(table_config_table, "updated_at", date, "id", 5)
    cls_2.close_connection_2()

def get_config():
    cls = table_management(hostname, dbname, uname, pwd)
    config_tupl = cls.fetch_one_result_filtered(table_config_table, 'config', 'id = 5')  # jeśli config == 1 to odpalamy, a jak 0 to nie
    cls.close_connection_2()
    config = config_tupl[0]
    return config

def updated_keywords():
    """
    do aktualizowania listy keywrdsów
    dni nieparzyste  - aktualizujemy config
    dni parzyste     - aktualizujemy słowa kluczowe
    """

    print("updated_keywors")
    if date.day % 2 == 1 and date.hour == 6:  # dzień przed aktualizacją zmieniamy config (w dni nieparzyste)
        config_value = get_config()
        updated_config(config_value)

    elif date.day % 2 == 0 and date.hour == 6:  # w dniu aktualizacji  #chwilowo na 15
        config_value = get_config()
        if config_value == 1:
            keywords = update_keywords()
            keywords.get_new_companies()  # nowe spółki
            keywords.get_shorter_time_update()  # nowe odmiany po krótszym okresie
            keywords.get_longer_time_update()  # nowe odmiany po dłuższym okresie
            updated_config_2()
