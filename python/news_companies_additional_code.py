import mysql.connector
import pandas as pd

from mysql_db import table_management
from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_all = config['table_news_companies_all']  #newsy w których są wzmianki o spółkach (unikalne)
table_news_companies_company = config['table_news_companies_company']  #newsy wzmianki o spółkach
table_news_companies_content = config['table_news_companies_content']  #treść artykułów, summary..
table_news_all = config['table_news_all']  #wszystkie pobrane newsy z gazet i portali w bazie
table_news_exception_wform = config['table_news_exception_wform']  #wyjątki słowne - analityk, ekspert..
table_zz_polish_word_dict = config['table_zz_polish_word_dict']  #odmiany polskich słów
table_news_companies_rejected = config['table_news_companies_rejected']  #odrzucone newsy
table_aalerts_backend = config['table_aalerts_backend']


#zmienne
news_min_comp_multi_news = int(config['news_min_comp_multi_news'])

#minimalna liczba spółek do wspomnienia w jednym artykule, aby został on zakwalifikowany do "omówienie spółek"
min_comp_multi_news = news_min_comp_multi_news


# tabela z danymi pomocnymi - feed newsów o spółce
#na końcu usunąć z tabeli news_companies_company kolumny z linkiem, title, display_text, sourceID, image ew date  i emitent (nie compID)
news_companies_company = ['newsID', 'sourceID', 'date', 'emitent', 'compID', 'title', 'display_text', 'link', 'hash_company', 'image', 'categoryID', 'mentioned', 'counter', 'score', 'new']
# tabela z danymi głównymi o artykule - główny feed z newsami
news_companies_all = ['sourceID', 'date', 'title', 'display_text', 'link', 'image', 'hash_article', 'categoryID', 'avg_reading', 'readable_index', 'score_news']
# tabela z danymi pełnymi o treści artykułu
news_companies_data = ['newsID', 'title', 'summary', 'article', 'link', 'extraction_score', 'words_in_art_perc', 'hash_article']
# tabela do newsów odrzuconych
news_companies_rejected = ['emitent', 'title', 'link', 'hash_article', 'date']


''' zapisywanie alertów do tabli w bazie danych '''

def alerts_table(info, date):
    ''' system alertowania w przypadku awarii pobierania szczegółowych danych o jakiejś spółce '''
    cls = table_management(hostname, dbname, uname, pwd)
    cls.add_data_row(table_aalerts_backend, [info, date, 'newsy'], '(info,updated,table_name)', '(%s, %s, %s)')
    cls.close_connection_2()



class check_word_table:  #do sprawdzania czy mamy już odmiany słów dostępne w bazie

    def __init__(self, comp_exception):
        self.comp_exception = comp_exception

    def get_keywords_comp(self):
        keywords_lst = self.comp_exception['keyword'].tolist()

        res = []
        for elem in keywords_lst:  #lista niepowtarzajacych się słów kluczowych - do któych muszą być odmiany w drugiej tabeli
            if elem not in res:
                res.append(elem)
        return res

    def get_keywords_forms(self):
        cls_0 = table_management(hostname, dbname, uname, pwd)
        word_forms = cls_0.get_columns_data(table_news_exception_wform, '*')  #sprawdzanie jakie już odmiany mamy w bazie
        cls_0.close_connection_2()

        keywords_lst = word_forms['base_word'].tolist()
        res = []
        for elem in keywords_lst:  #lista niepowtarzajacych się słów dla których są odmiany w bazie
            if elem not in res:
                res.append(elem)
        return res

    def check_availability(self):
        comp_kwrd_lst = check_word_table.get_keywords_comp(self)  #słowa które są wyjątkami dla spółek
        word_kwrd_lst = check_word_table.get_keywords_forms(self)  #słowa dla których są odmiany w baze

        new_base_words = []
        for elem in comp_kwrd_lst:
            if elem not in word_kwrd_lst:
                new_base_words.append(elem)

        if len(new_base_words) > 0:
            check_word_table.update_table_words(self, new_base_words)  #updatowanie bazy o nowe słowa - odmiany

    def update_table_words(self, new_base_words):
        cls_0 = table_management(hostname, dbname, uname, pwd)
        word_variance = cls_0.get_multi_filtered_columns_df(table_zz_polish_word_dict, '*', f'base_word IN {tuple(new_base_words)}')  # słownik odmian słów
        for keyword in new_base_words:  #dla kazdego nowego słowa którego odmiany musimy dodać do tabeli
            df_keyword = word_variance[word_variance['base_word'] == keyword]
            df_keyword = df_keyword.drop(['id'], axis=1)
            for form in df_keyword['form'].tolist():
                cls_0.add_data_row('news_exception_wform', [keyword, form], '(base_word,form)', '(%s, %s)')
        cls_0.close_connection_2()


''' zapisywanie newsów odrzuconych '''

def save_rejected_news(df_del):  #funkcja do zapisywania odrzuconych newsów - żeby sprawdzać czy dobre
    df_del = df_del[news_companies_rejected]
    df_del = df_del.astype(object).where(pd.notnull(df_del), None)
    df_del = df_del.drop_duplicates(subset=['hash_article'], keep='last')

    cls_3 = table_management(hostname, dbname, uname, pwd)
    cls_3.insert_df(df_del, table_news_companies_rejected, 'append', False)  #zapisywanie odrzuconych newsów, jesli nie ma ich w bazie
    cls_3.close_connection_2()


''' aktualizowanie newsów, w których nie było wzmianek o spółkach '''

def udapte_news_new():
    """
    Funkcja do zmieniania kolumny new z 1 na 0 dla wszystkich newsów po ich analizie
    """
    cls = table_management(hostname, dbname, uname, pwd)
    cls.set_column_value(table_news_all, 'new', 0)
    cls.close_connection_2()


''' określanie kategorii newsa - omówienie spółek czy o działalności spółek (wg ilości wzmianek różnych spółek w danym newsie) '''

class news_category:

    def __init__(self, news_rss):
        self.news_rss = news_rss

    def group_same_news(self):
        grouped = self.news_rss.groupby("link").size().sort_values(ascending=False)
        most_repeated = grouped[grouped >= min_comp_multi_news]
        return most_repeated.index.tolist()

    def determine_category(self):
        df_categorized = self.news_rss.copy()
        most_rep_lst = self.group_same_news()

        df_categorized['categoryID'] = df_categorized.apply(lambda row: 1 if row['link'] in most_rep_lst else 2, axis=1)
        return df_categorized


''' zapisywanie do bazy danych unikalnych newsów ze spółkami '''


def save_news_companies_all(df):
    """
    Funkcja do zapisywania news_companies_company
    :param df: dataframe ze wszystkimi danymi o newsach
    :return: zwracany jest słownik z newsID dla każdego hash_article
    """
    df = df.rename(columns={"score": "score_news"})
    df_unique = df.sort_values(by='score_news', ascending=True).drop_duplicates(subset=['hash_article'], keep='last').sort_values(by='date')#.reset_index(drop=True)

    df_unique['date'] = df_unique['date'].astype(str)
    df_unique = df_unique[news_companies_all]

    df_unique = df_unique.astype(object).where(pd.notnull(df_unique), None)
    #df_unique['new'] = 1
    rows_dict_lst = df_unique.to_dict('records')

    col_names = list(df_unique.columns)
    col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
    values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"

    newsID_dict = {}
    cls = table_management(hostname, dbname, uname, pwd)
    for dict_data in rows_dict_lst:
        hash_article = dict_data['hash_article']
        try:
            data = list(dict_data.values())
            cls.add_data_row('news_companies_all', data, col_names_string, values_string)  # dodać index
        except mysql.connector.IntegrityError:
            print(f">>>> Alert! Podany artykuł już istnieje w bazie: {dict_data['hash_article']}")

        newsID = cls.fetch_one_result_filtered(table_news_companies_all, 'id', f"hash_article = '{hash_article}'")  #czy artykuł już jest czy nie to i tak wyciągamy jego newsID
        newsID_dict[hash_article] = newsID[0]
    cls.close_connection_2()
    return newsID_dict


def save_news_data_content(df):
    """
    Funkcja do zapisywania news_companies_data
    :param df: dataframe ze wszystkimi danymi o newsach
    """
    df_unique = df.drop_duplicates(subset=['hash_article'], keep='last').reset_index(drop=True)
    df_unique = df_unique[news_companies_data]

    df_unique = df_unique.astype(object).where(pd.notnull(df_unique), None)
    rows_dict_lst = df_unique.to_dict('records')

    col_names = list(df_unique.columns)
    col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
    values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"

    cls = table_management(hostname, dbname, uname, pwd)
    for dict_data in rows_dict_lst:
        try:
            data = list(dict_data.values())
            cls.add_data_row(table_news_companies_content, data, col_names_string, values_string)  # dodać index
        except mysql.connector.IntegrityError:
            print(f">>>> Alert! Podany artykuł już istnieje w bazie: {dict_data['hash_article']}")
    cls.close_connection_2()


def save_news_companies_company(df):
    """
    Funkcja do zapisywania news_companies_company - wzmianki o spółkami rozbite na newsy
    :param df: dataframe ze wszystkimi danymi o newsach
    """
    df['date'] = df['date'].astype(str)
    df['new'] = 1
    df = df.sort_values(by='date').reset_index(drop=True)  #w bazie zapisujemy newsy według daty publikacji - żeby potem w feed były chronologicznie
    df_unique = df[news_companies_company]

    df_unique = df_unique.astype(object).where(pd.notnull(df_unique), None)
    rows_dict_lst = df_unique.to_dict('records')

    col_names = list(df_unique.columns)
    col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
    values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"

    cls = table_management(hostname, dbname, uname, pwd)
    for dict_data in rows_dict_lst:
        try:
            data = list(dict_data.values())
            cls.add_data_row(table_news_companies_company, data, col_names_string, values_string)  # dodać index
        except mysql.connector.IntegrityError:
            print(f">>>> Alert! Podany artykuł już istnieje w bazie: {dict_data['hash_company']}")
    cls.close_connection_2()

