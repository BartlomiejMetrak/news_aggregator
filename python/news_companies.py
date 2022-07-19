import pandas as pd

from config import hashing_SHA2, config
from mysql_db import table_management
from news_similarity import news_sim
from news_find_companies import find_company
from news_importance import find_important_news
from news_companies_reject import rejected_news
from news_scoring_display import rank_articles, rank_similar_all_news
from news_companies_additional_code import (news_category, save_news_companies_all, save_news_companies_company,
                                            udapte_news_new, save_news_data_content, save_rejected_news)


pd.set_option('display.max_columns', None)

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_all = config['table_news_all']  #wszystkie pobrane newsy z gazet i portali w bazie
table_sources_news_rss = config['table_sources_news_rss']  #źródła
table_news_companies_all = config['table_news_companies_all']  #newsy w których są wzmianki o spółkach (unikalne)
table_news_companies_rejected = config['table_news_companies_rejected']  #odrzucone newsy


'''
1. pobieramy nowe newsy z tabeli ogólnej dla artykułów
2. filtrujemy po liście żródeł do giedły - z tabeli w db
3. pozostałe zamieniamy new z 1 na 0 (te które są gazety zamieniamy dopiero w chwili wgrywania do bazy danych)
4. pobrane newsy analizujemy
'''


def find_freq(row, sorted):
    link, compID = row['link'], row['compID']
    return sorted.loc[compID, link]['counter']

def count_mentions(df):
    groupped = df.groupby(['compID', 'link']).sum()
    sorted = groupped.sort_values(by='counter', ascending=False)

    df_unique = df.sort_values(by='mentioned', ascending=True).drop_duplicates(subset=['compID', 'link'], keep='last').reset_index(drop=True)  #sortujemy po mentioned, żeby wziąć najwyższą wartość
    df_unique['counter'] = df_unique.apply(lambda row: find_freq(row, sorted), axis=1)
    return df_unique #df_unique.sort_values(by='counter').reset_index(drop=True)

def search_companies_in_text(df_news_companies):
    df_news_companies = df_news_companies.fillna('missing')
    cls = find_company(df_news_companies)  #0 if searched by title, description and first 90% word of article, 1 if searched by title and desc
    df_comp_summary_2 = cls.find_comp()

    if df_comp_summary_2.empty is True:
        udapte_news_new()
        print(f'Brak spółek wykrytych w newsach, długość df: {len(df_comp_summary_2)}')
        quit()
    else:
        return count_mentions(df_comp_summary_2)

def hash_company(row):
    string_to_hash = row['hash_article'] + str(row['compID'])
    hash = hashing_SHA2(string_to_hash)
    return hash

def get_news_hash(df):
    df['hash_company'] = df.apply(lambda row: hash_company(row), axis=1)
    return df

def apply_newsID(df, newsID_dict):
    df['newsID'] = df.apply(lambda row: newsID_dict[row['hash_article']] if pd.notnull(row['hash_article']) else pd.NA, axis=1)
    return df

def filter_data(df, table_name):
    """
    funkcja do odfiltrowywania newsów które już są zapisane w tabeli do newsów o spółkach, (zarówno tabele cleaned i rej)
    żeby nie podbijać PK id poźniej
    Tak dla pewności bo i tak kolumna new jest zamieniana z 1 na 0
    Odfiltrowywane są tylko te, w których zostały wykryte wzmianki czyli np ze 100 newsów 13 o spółkach to powinno zostać 87
    """
    date_start = df['date'].min()
    hash_article_lst = df['hash_article'].tolist()

    cls_0 = table_management(hostname, dbname, uname, pwd)
    hash_article_lst = [str(x) for x in hash_article_lst]  #nie może być zero bo wcześniej ograniczamy to
    hash_article_tpl = '("' + '","'.join(hash_article_lst) + '")'
    hashes_in_db = cls_0.fetch_all_results_filtered(table_name, 'hash_article',
                                                    f'date(date) >= "{date_start.date()}" AND hash_article IN {hash_article_tpl}')  # newsy, które już były zapisane do bd
    cls_0.close_connection_2()

    hashes_in_db = [x[0] for x in hashes_in_db]
    df_final = df[~df['hash_article'].isin(hashes_in_db)]
    return df_final


def update_rss_news_comp():  #tylko nowe newsy
    """
    1. bierzemy nowe newsy z tabeli news_all >> są to wszystkei newsy pobrane do bazy z rss_feed
    2. odrzucamy gazety nie brane pod uwagę dla wzmianek o spółkach
    3. wyszukujemy wzmianki o spółkach i obliczamy współczynniki (większy df jeśli newsy się powielają
    4. odrzucamy wzmianki o spółkach z odrzuconymi keywordsami (typu ekonomiści ING...)
    5. oznaczamy kategorię newsów >> omówienie spółek lub działalność
    6. dodajemy hash dla tabeli news_companies_company oraz newsID dla newsów z tabeli głównej news_companies_all
    7. zapisujemy newsy do tabeli o wzmiankach spółek oraz newsy do tabeli z pełna treścią newsów (article i summary)
    8. obliczamy wskaźnik podobieństwa do innych newsów z tabeli news_companies_company (na podstawie article i summary (title musi byc 98%))

    """

    cls_0 = table_management(hostname, dbname, uname, pwd)
    df_news_rss_all = cls_0.get_multi_filtered_columns_df(table_news_all, '*', 'new = 1')  #najnowsze newsy ze wszystkich rss_feed plus są w rss_feed liscie giełdowej
    rss_feed_sources = cls_0.fetch_all_results_filtered(table_sources_news_rss, 'id', 'feed_companies = 1')  #pobieramy id feedów które mają wartość 1 dla kolumny feed_..
    cls_0.close_connection_2()

    rss_feed_sources = [x[0] for x in rss_feed_sources]
    df_news_companies = df_news_rss_all[df_news_rss_all['sourceID'].isin(rss_feed_sources)]  #newsy z gazet branych pod uwagę

    if len(df_news_companies) == 0:
        print("Brak nowych newsów w RSS feed")
        quit()

    #df_news_companies = df_news_companies.iloc[0:100]
    df_news_companies = filter_data(df_news_companies, table_news_companies_all)

    print("\n\nWyszukiwanie wzmianek o spółkach w newsach...")
    df = search_companies_in_text(df_news_companies) #.head(100))  #wyszukiwanie wzmianek o spółkach w tekście + liczba i miejsce(tytuł, desc..) tych wzmianek
    df_rej, df_cleaned = rejected_news(df)

    if len(df_rej) != 0:
        """ zapisywanie artykułów odrzuconych """
        df_rej_2 = filter_data(df_rej, table_news_companies_rejected)
        if len(df_rej_2) != 0:
            print(f"Liczba odrzuconych artykułów to: {len(df_rej_2)}")
            save_rejected_news(df_rej_2)

    print(f"Liczba nowych artykułów ze spółkami: {len(df_cleaned)}\n")
    if len(df_cleaned) != 0:  #jeśli nie ma żadnych nowych wzmianek to nie obliczamy dalej

        cls_1 = news_category(df_cleaned)
        df_cleaned_categorized = cls_1.determine_category()  # określanie kategorii newsa - omówienie spółek lub działalność
        df_cleaned_categorized = get_news_hash(df_cleaned_categorized)

        """ zapisywanie danych ogólnych o newsach (nie o konkretnych wzmiankach spółek """
        print("Zapisywanie treści newsów do bazy...")
        newsID_dict = save_news_companies_all(df_cleaned_categorized)  # zwracany jest słownik z newsID, score_news oraz importance_news

        """ dataframe z newsID newsa """
        df_news_company = apply_newsID(df_cleaned_categorized, newsID_dict)  # dataframe z dodaną kolumną newsID
        save_news_data_content(df_news_company)

        """ zapisywanie artykułów wg wzmianek o spółkach """
        save_news_companies_company(df_news_company)

        """ okreslanie podobieństw, ważności i rank newsów (z poziomu spółek)"""
        df_cleaned_categorized['date'] = pd.to_datetime(df_cleaned_categorized['date'])  #zamiana na format datetime
        first_date = df_cleaned_categorized['date'].min()  #pierwsza data w tabeli

        similarity = news_sim(df_cleaned_categorized, first_date)
        similarity.calculate_sim()

        importance = find_important_news(first_date)
        importance.group_most_popular()

        rank_articles(first_date)
        rank_similar_all_news(first_date)

    """ zamiana kolumny new = 0 z wszystkich newsów tabeli news_all """
    udapte_news_new()


    '''   
    ** działanie algorytmu >> to tabeli głównej z news_all dodać kolumne - spółki wzmiankowane w tym artykule, 
    najwyższy wskaźnik importance z tabeli news_companies oraz do zdecydowania - który pierwszy artykuł oraz podobne artykuły
    
    *** przemyśleć co z tworzeniem się łańccha artykułów podobnych - może jakoś to odciąć - że max do x dni wstecz i potem starsze zamieniamy czy coś
        lub wyciągamy wszystkie podobne artykuły do obliczenia rank
        
    **** artykuły omówienie spółek mogą być podobne do siebie tylko w danym dniu - zmienić - id 158
    ***** mentioned - czy nie jest tak że przy różnych wzmiankach może być w różnym miejscu mentioned - sprawdzić agora id 178 newsy
    '''
