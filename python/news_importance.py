from datetime import timedelta, datetime
import pandas as pd

from mysql_db import table_management
from config import config
#pd.set_option('display.max_rows', None)


'''
Aglorytm służy do wyszukiwania ważniejszych newsów - tych które się powtarzają minimum x razy w różnych mediach w ciągu ostatnich 4 dni
Wyszukujemy za ostatnie 4 dni bo zakładamy, że newsy odgrzane z przeszłości nie są gorącym tematem

Przeliczany jest po każdym pobraniu nowych newsów z gazet

Zmieniamy tylko jeżeli news jest uznany za ważny - nie ma zmiany odwrotnej - że był ważny a teraz już nie jest

do zrobienia:
dodać ograniczenie że tylko newsy o randze 1 mogą zostać zamienione na rangę 2 - wzmianki w artykule się nie liczą bo nie jest o tej spółce news (zmienić wiersz 59 ?)
i omówienie spółek nie może mieć wagi 1 lub 2 - dodać zmianę

importance
0 >> newsy omawiające różne spółki
1 >> newsy w których wystepuje tylko wzmianka o spółce nie w tytule i summary lub w tytule i summary lecz z max 1 wzmianką w artykule
2 >> newsy o spółce - wzmianka o spółce pojawia się w tytule lub summary, min 2 wzmianki w treści artykułu
3 >> popularny temat - wzmianka o spółce w tytule lub summary i news jest w grupie podobnych min. 3 newsów

'''

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_company = config['table_news_companies_company']  #newsy wzmianki o spółkach

#zmienne
news_rep_of_imp_news = int(config['news_rep_of_imp_news'])
news_n_days_before = int(config['news_n_days_before'])


rep_of_imp_news = news_rep_of_imp_news
n_days_before = news_n_days_before  #x dni przed datą publikacji newsa ten sam co w similarity


class find_important_news:
    '''
    klasa do określania ważności newsów, wszystkei stopnie za wyjątkiem 3 są określane na podstawie jednego newsa
    3 stopień - popularny temat jest określany z użyciem historycznych wartości poprzenich podobnych newsów,
    dlatego przy tym moze ulec zmianie wartość poprzednich newsów w tabeli

    Update jest tylko do nowych newsów, które nie maja jeszcze wartości importance (jako że i tak bierzemy newsy nowe plus z ostatnich 5 dni)
    Do updatu dołączane są id newsów z tabeli, które otrzymały importance = 3
    '''

    def __init__(self, the_earliest_news_date):
        self.the_earliest_news_date = the_earliest_news_date

    def get_last_final_news(self):
        date_n_days_ago = self.the_earliest_news_date - timedelta(days=n_days_before)  #data n dni przed najwcześniejszą datą w dataframie

        cls = table_management(hostname, dbname, uname, pwd)

        condition = f'date(date) >= "{date_n_days_ago.date()}"'
        df = cls.get_multi_filtered_columns_df(table_news_companies_company, 'id,similarity,categoryID,mentioned,counter,importance', condition)  # pobieranie newsów z ostatnich 4 dni zamienić na the_earliest_news_date
        return df

    def group_most_popular(self):
        df = self.get_last_final_news()
        idx_lst_to_change = df[df['importance'].isna()]['id'].tolist()  #lista id newsów, które jeszcze nie były określane do podobieństwa i mają ważność NULL
        df = df.sort_values(by='categoryID')

        df.loc[df['categoryID'] == 1, "importance"] = 0  #newsy omawiające różne spółki
        df.loc[(df['mentioned'] == 0) & (df['categoryID'] == 2), "importance"] = 1  #wzmianka o spółce
        df.loc[(df['mentioned'] == 1) & (df['categoryID'] == 2) & (df['counter'] <= 2), "importance"] = 1  # wzmianka o spółce
        df.loc[(df['mentioned'] == 1) & (df['categoryID'] == 2) & (df['counter'] > 2), "importance"] = 2  #newsy o spółce

        var = rep_of_imp_news - 1  #bo jeszcze jest news podobny

        df_hot_topic = df[df['categoryID'] != 1]  #popularny tematem nie może być news omawiający spółki, tylko o działalnosci
        grouped = df_hot_topic.groupby("similarity").size().sort_values(ascending=False)
        most_repeated = grouped[grouped >= var]

        if len(most_repeated) != 0:
            hot_topic_id_lst = self.estimate_importance(most_repeated, df_hot_topic)  #jeśli są jakiekolwiek podobne newsy to wtedy wywołujemy tą klasę
            df.loc[df['id'].isin(hot_topic_id_lst), "importance"] = 3  #!! ważny temat :)
            idx_lst_to_change = list(set(idx_lst_to_change + hot_topic_id_lst))  #lista id do aktualizacji wliczając w to historyczne newsy zmienione na 3

        df_to_update = df[df['id'].isin(idx_lst_to_change)]  #dataframe do aktualizacji - nowe newsy plus stare zamienione na popularny temat

        columns = list(df_to_update.columns)
        newsID = columns.index('id') + 1
        importanceID = columns.index('importance') + 1

        cls = table_management(hostname, dbname, uname, pwd)
        for ir in df_to_update.itertuples():
            cls.update_value(table_news_companies_company, 'importance', ir[importanceID], 'id', ir[newsID])
        cls.close_connection_2()

    def estimate_importance(self, most_repeated, df):
        most_repeated = most_repeated.drop([0])  # odrzucamy wartość równą 0
        idx_lst = most_repeated.index.tolist()

        hot_topic_id_lst = idx_lst  #wypadkowa lista to id newsów podobnych

        ids_to_change = []

        for idx in idx_lst:
            similarities_lst = df[df['similarity'] == idx]['id'].tolist()  #wyszukiwanie tych newsów które mają sim_sent równy x

            main_id_to_change = df[(df['id'] == idx) & (df['importance'] != 3)]['id'].tolist()
            id_to_change = df[(df['similarity'] == idx) & (df['importance'] != 3)]['id'].tolist()
            ids_to_change = ids_to_change + main_id_to_change + id_to_change

            hot_topic_id_lst = hot_topic_id_lst + similarities_lst
        return ids_to_change
