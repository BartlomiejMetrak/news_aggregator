from mysql_db import table_management
from datetime import timedelta
import random

from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_all = config['table_news_companies_all']  #newsy w których są wzmianki o spółkach (unikalne)
table_news_companies_company = config['table_news_companies_company']  #newsy wzmianki o spółkach
table_sources_news_rss = config['table_sources_news_rss']  #źródła

#zmienne
news_n_days_before = int(config['news_n_days_before'])


n_days_before = news_n_days_before  #5 dni przed datą publikacji newsa ten sam co w similarity


'''
Algorytm do obliczania scoringu dla newsów według, którego określany będzie pierwszy artykuł z grupy podobnych

Zmienne brane pod uwagę:
1. avg reading time
2. flasch score - readable
3. source
4. mentioned

AVG:
0.5 min : 0 pkt
1-2 min : 3 pkt
>=3     : 0 pkt
> 10    : -1 pkt

FLESCH SCORE
< -10   : -2 pkt
-10 - 5 : -1 pkt
-5 - 5  : 0 pkt
5 - 15  : 1 pkt
> 15    : 2 pkt

MENTIONED (miara do określania istotności newsa)
title i summary (1) : 2 pkt
article (0)         : 0 pkt

SOURCES
punktacja w tabeli

'''

def get_source_score():
    cls = table_management(hostname, dbname, uname, pwd)
    source_lst = cls.fetch_all_results(table_sources_news_rss, 'id,point_score')
    cls.close_connection_2()
    source_dict = {k[0]: k[1] for k in source_lst}
    return source_dict


class calculate_scores:
    '''
    klasa do obliczania score dla artykułu
    SCORE obliczany jest według artykułu to znaczy że nie potrzebne są historyczne artykuły do jego obliczenia
    source_dict >> słownik z punktacją przydzielaną dla poszczególnych portali / gazet
    '''

    source_dict = get_source_score()

    def __init__(self, row):
        self.mentioned = row['mentioned']
        self.sourceID = row['sourceID']
        self.avg_reading = row['avg_reading']
        self.readable_index = row['readable_index']

    def get_readable_index_points(self):
        if bool(self.readable_index) is False:
            return 0
        elif self.readable_index <= -10:
            return -2
        elif -10 < self.readable_index <= -5:
            return -1
        elif -5 < self.readable_index <= 5:
            return 0
        elif 5 < self.readable_index <= 15:
            return 1
        elif self.readable_index > 15:
            return 3
        else:
            return 0

    def get_avg_reading_points(self):
        if bool(self.avg_reading) is False:
            return 0
        elif self.avg_reading <= 0.5:
            return 0
        elif 1 <= self.avg_reading <= 2:
            return 3
        elif 3 <= self.avg_reading <= 9:
            return 0
        else:
            return -1

    def get_mentioned_points(self):
        if self.mentioned == 1:
            return 2
        else:
            return 0

    def get_source_points(self):
        return self.source_dict[self.sourceID]


def calculate_article_score(row):
    scores = calculate_scores(row)

    mentioned_points = scores.get_mentioned_points()
    avg_reading_points = scores.get_avg_reading_points()
    readable_index_points = scores.get_readable_index_points()
    source_points = scores.get_source_points()
    random_number = random.uniform(0, 0.8)  #dzięki temu dwa newsy o score = 4 będą miały inne wartości, ale nie większe od newsa o score = 5

    article_score = source_points + readable_index_points + mentioned_points + avg_reading_points + round(random_number, 2)
    return article_score


''' obliczanie rank artykułu '''

def get_old_sim_news_2(the_earliest_news_date):
    date_n_days_ago = the_earliest_news_date - timedelta(days=n_days_before)  # data n dni przed najwcześniejszą datą w dataframie

    cls = table_management(hostname, dbname, uname, pwd)
    news_companies_company = table_news_companies_company
    news_companies = table_news_companies_all
    where_condition = f'{news_companies_company}.newsID = {news_companies}.id AND date({news_companies_company}.date) >= "{date_n_days_ago.date()}"'  # AND {news_companies_company}.new = 0'
    cols_1 = ['id', 'mentioned', 'sourceID', 'similarity', 'art_rank', 'score']
    cols_2 = ['avg_reading', 'readable_index']

    df = cls.fetch_data_multi_tables(table_news_companies_company, table_news_companies_all, cols_1, cols_2, where_condition)
    cls.close_connection_2()
    return df

def rank_articles(the_earliest_news_date):
    '''
    funkcja do określania rank artykułu na podstawie zebranego score i artykułów podobnych
    RANK obliczany jest również z uwzględnieniem grupy podobnych artykułów
    '''
    df_raw = get_old_sim_news_2(the_earliest_news_date)  #pobieramy newsy z bazy za ostatni okres
    df = df_raw.copy()
    df['art_rank'] = 1  #początkowo wszystkie artykuły mają rank równy 1  aby to było poprawne to należy dodać df z jeszczep odobnymi dalej historycznie newsami
    ids = list(df['similarity'].unique())  #wszystkie grupy artykułów podobnych
    ids.remove(0)

    df = df.sample(frac=1)

    for id in ids:
        filtered_df = df[(df['similarity'] == id) | (df['id'] == id)].copy()  #wszystkie id artykułów podobnych z danej grupy
        filtered_df["art_rank"] = filtered_df["score"].rank(method='first', ascending=False)  #rank artykułów z jednej grupy

        df.loc[filtered_df.index, ['art_rank']] = filtered_df[['art_rank']]  #zmiana ranku dla artykułów w analizowanym zakresie dat

    df = df.sort_index()

    df_final = df['art_rank'].compare(df_raw['art_rank'], align_axis='index')  #updatujemy tylko te wiersze, w których są zmiany
    ids_to_change = list(set(df_final.index.get_level_values(0)))

    df_filtered = df[df.index.isin(ids_to_change)]
    rank_list = list(zip(df_filtered['id'], df_filtered['art_rank']))
    print(f"\nCałkowita ilość zmienionych wierszy (rank/sim): {len(rank_list)}")

    cls = table_management(hostname, dbname, uname, pwd)
    for row in rank_list:
        cls.update_value(table_news_companies_company, "art_rank", row[1], "id", row[0])
    cls.close_connection_2()


''' problem do rozwiązania: artykuły na końcu daty w pewnym momencie mogą zostać same i ich rank
 zmieni się na 1 mimo że maja wcześniej podobne artykuły >> grupa podobnych z jednej daty rozwiązuje ten problem '''


''' algorytm to stopniowania głównego newsfeedu '''

def calculate_main_news_rank(df_raw):
    df = df_raw.copy()
    df['art_rank_main'] = 1
    ids = list(df['similarity_main'].unique())  # wszystkie grupy artykułów podobnych

    if 0 in ids:
        ids.remove(0)

    df = df.sample(frac=1)
    for id in ids:
        filtered_df = df[(df['similarity_main'] == id) | (df['newsID'] == id)].copy()  # wszystkie id artykułów podobnych z danej grupy
        filtered_df["art_rank_main"] = filtered_df["score_news"].rank(method='first', ascending=False)  # rank artykułów z jednej grupy

        df.loc[filtered_df.index, ['art_rank_main']] = filtered_df[['art_rank_main']]  # zmiana ranku dla artykułów w analizowanym zakresie dat

    df = df.sort_index()
    return df

def calculate_importance_main(df_rank, df):
    df_importance = df.sort_values(by='importance', ascending=True).drop_duplicates(subset=['newsID'], keep='last')
    df_rank['importance_main'] = df_rank.apply(lambda row: df_importance.loc[df_importance['newsID'] == row['newsID'], 'importance'].iloc[0], axis=1)
    return df_rank

def get_old_sim_news(the_earliest_news_date):
    date_n_days_ago = the_earliest_news_date - timedelta(days=n_days_before)  # data n dni przed najwcześniejszą datą w dataframie

    cls = table_management(hostname, dbname, uname, pwd)
    news_companies_company = table_news_companies_company
    news_companies = table_news_companies_all
    where_condition = f'{news_companies_company}.newsID = {news_companies}.id AND date({news_companies_company}.date) >= "{date_n_days_ago.date()}"'  # AND {news_companies_company}.new = 0'
    cols_1 = ['id', 'score', 'newsID', 'similarity', 'emitent', 'art_rank', 'importance']
    cols_2 = ['art_rank_main', 'similarity_main', 'score_news', 'importance_main']

    df = cls.fetch_data_multi_tables(table_news_companies_company, table_news_companies_all, cols_1, cols_2, where_condition)
    cls.close_connection_2()
    return df

def rank_similar_all_news(the_earliest_news_date):
    df = get_old_sim_news(the_earliest_news_date)

    ids = list(df['newsID'].unique())  #lista unikalnych id newsów
    df_rank = df.copy()
    df_rank = df_rank.drop_duplicates(subset=['newsID'], keep='last')  #newsy unikalne - news_companies_all
    df_news_old = df_rank.copy()  #kopia tabeli głównej z newsami

    for id in ids:  #dla id z ids unikalnych newsów w tabeli news_companies_all
        id_news_company_lst = df[(df['newsID'] == id)]['id'].tolist()  # ids newsa z tabeli news_companies_company > dla różnych wzmianek o spółkach
        sim_sim_ids = df[(df['newsID'] == id) & (df['similarity'] != 0)]['similarity'].tolist()
        ids_group = list(set(id_news_company_lst + sim_sim_ids))
        sim_ids = df[(df['id'].isin(ids_group)) | (df['similarity'].isin(ids_group))]['newsID'].unique().tolist()  # lista newsów podobnych do danego newsa z tabeli news_companies_all

        if 0 in ids_group:
            ids_group.remove(0)

        if len(sim_ids) > 1:  #jesli są jakieś podobne newsy oprócz samego siebie to...
            similar_newsID = df_rank.loc[df_rank['newsID'] == id, 'similarity_main'].iloc[0]
            if similar_newsID != 0:
                sim_ids_similar = [x for x in sim_ids if x != similar_newsID and x != id]
                df_rank.loc[df_rank['newsID'].isin(sim_ids_similar), 'similarity_main'] = similar_newsID
            else:
                sim_ids_similar = [x for x in sim_ids if x != id]  # bez id newsa samego siebie
                df_rank.loc[df_rank['newsID'].isin(sim_ids_similar), 'similarity_main'] = id

    df_rank = calculate_main_news_rank(df_rank)
    df_rank = calculate_importance_main(df_rank, df)

    " teraz tylko wyfiltrować te rank i similarity które się zmieniły i zupdatować w bazie "
    df_final = df_rank.compare(df_news_old, align_axis='index')

    ids_to_change = list(set(df_final.index.get_level_values(0)))

    df_filtered = df_rank[df_rank.index.isin(ids_to_change)]
    rank_list = list(zip(df_filtered['newsID'], df_filtered['art_rank_main'], df_filtered['similarity_main'], df_filtered['importance_main']))

    cls = table_management(hostname, dbname, uname, pwd)
    for row in rank_list:
        cls.update_value(table_news_companies_all, "art_rank_main", row[1], "id", row[0])
        cls.update_value(table_news_companies_all, "similarity_main", row[2], "id", row[0])
        cls.update_value(table_news_companies_all, "importance_main", row[3], "id", row[0])
    cls.close_connection_2()
