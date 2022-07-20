from textstat.textstat import textstat
from nltk.tokenize import sent_tokenize
import pandas as pd
import string
import re

from news_scoring_display import calculate_article_score
from mysql_db import table_management
from config import config

pd.set_option('display.max_columns', None)



#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_companies = config['table_companies']
table_news_companies_keywords = config['table_news_companies_keywords']  #słowa kluczowe słów
table_news_companies_keywords_excluded = config['table_news_companies_keywords_excluded']  #słowa wykluczone jak OC i AC, AC Milan


#zmienne
news_display_text_chars = int(config['news_display_text_chars'])

'''
1. pobieramy nowe newsy z tabeli ogólnej dla artykułów
2. filtrujemy po liście żródeł do giedły - z tabeli w db
3. pozostałe zamieniamy new z 1 na 0 (te które są gazety zamieniamy dopiero w chwili wgrywania do bazy danych)
4. pobrane newsy analizujemy
'''


display_text_chars = news_display_text_chars


''' analizowanie treści artykułów '''

def calculate_readable_index(text):
    '''
    Funkcja do obliczania wskażnika czytelności - dla polskich tekstów jest on zdeycdowanie niższy niż dla angielskich
    Jednak powienna zostać zachowana zależność między tekstami polskimi.
    Wskaźnik wykorzystywany jest później do określania, który artykuł jest wyświetlany pierwszy
    :param text: tekst artykułu
    '''
    total_sent = len(sent_tokenize(text))
    total_words = textstat.lexicon_count(text, removepunct=True)
    textstat.set_lang('pl')
    total_syllables = textstat.syllable_count(text=text)

    flesch_readable_formula = 206.835 - 1.015 * (total_words / total_sent) - 84.6 * (total_syllables / total_words)
    return round(flesch_readable_formula)

def avg_reading_time(text):
    avg_time = textstat.reading_time(text, ms_per_char=55) / 100
    if avg_time <= 0.75:
        return 0.5
    else:
        return round(avg_time)

def text_processing(text):
    if bool(text) is False or text == 'missing':  #jeśli jest pusty lub missing
        return None, None
    else:
        avg_time = avg_reading_time(text)
        readable_index = calculate_readable_index(text)
        return avg_time, readable_index


''' przycinanie tekstu artykułu do pierwszy 90% słów '''

def truncate_string(string):
    string_words = string.split(' ')
    lgt = len(string_words)
    stop = int(0.9 * lgt)
    return ' '.join(string_words[:stop])


''' tekst do wyświetlania w feedzie '''

def get_display_text(string_object):
    if len(string_object) > display_text_chars - 30:  #jeśli tekst jest dłuższy to obcinamy i dodajemy 3-kropki
        string_object = string_object.rsplit(' ', 1)[0] + "..."
    string_object = re.sub(r"\s+", ' ', string_object)
    return string_object

def get_unique_keywords_active():
    """ pobieramy dataframe unikalnych keywordsów dla aktywnych spółek giełowych bez tych nie zatwierdzonych recznie """
    cls = table_management(hostname, dbname, uname, pwd)
    table_1 = table_news_companies_keywords
    table_2 = table_companies
    where_condition = f'{table_1}.compID_f = {table_2}.id AND {table_2}.active = 1 AND {table_1}.new_keyword = 0'
    cols_2 = []
    cols_1 = ['id', 'compID_f', 'emitent', 'keyword', 'case_sensitive', 'new_keyword', 'to_be_updated', 'updated_at']
    df_keyword = cls.fetch_data_multi_tables(table_1, table_2, cols_1, cols_2, where_condition)
    cls.close_connection_2()
    df_keyword = df_keyword.drop_duplicates(subset=['compID_f', 'keyword'])
    return df_keyword

def get_unique_keywords_excluded():
    """ pobieranie dataframe blokowanych keywordsów dla aktywnych spółek giełdowych """
    cls = table_management(hostname, dbname, uname, pwd)
    table_1 = table_news_companies_keywords_excluded
    table_2 = table_companies
    where_condition = f'{table_1}.compID_f = {table_2}.id AND {table_2}.active = 1'
    cols_2 = []
    cols_1 = ['id', 'compID_f', 'emitent', 'keyword_excluded']
    df_keyword_excluded = cls.fetch_data_multi_tables(table_1, table_2, cols_1, cols_2, where_condition)
    cls.close_connection_2()
    return df_keyword_excluded

def check_upper_case(company_name, text_cleaned, text_upper):
    """ sprawdzanie czy odnalezione hasło zawiera duże litery - nazwa firmy """
    indices = [[i.start(), i.end()] for i in re.finditer(company_name, text_cleaned)]
    for word in indices:
        start = word[0]
        end = word[1]
        company_name = text_upper[start:end]
        if any(x.isupper() for x in company_name):
            return True
    return False


class find_company:  # do wyszukiwania wzmianek o spółkach - brane są pod uwagę różne odmiany nazw spółek

    def __init__(self, rss_news):
        self.keywords_all = get_unique_keywords_active()
        self.keywords_excluded = get_unique_keywords_excluded()
        self.rss_news = rss_news  # dataframe z frame z najnowszymi newsami z rss feed z bd

    def find_comp(self):
        frame = []  # do końcowej tabeli

        news_word_lst_lower = self.news_df_new(0)  #dla spółek bez case_sensitivity
        news_word_lst_no_change = self.news_df_new(1)  #dla spółek z case_sensitivity

        columns = list(self.keywords_all.columns)
        keyword_id = columns.index('keyword') + 1
        case_sensitive_id = columns.index('case_sensitive') + 1
        compID_f_id = columns.index('compID_f') + 1
        emitent_id = columns.index('emitent') + 1

        for ir in self.keywords_all.itertuples():  # dla każdego z keywordsów dostępnych w bd w tabeli companies_keywords
            comp_name = ir[keyword_id]
            comp_case_sensitivity = ir[case_sensitive_id]
            comp_name = " " + comp_name + " "

            if comp_case_sensitivity == 0:  #jeśli spółka w kolumnie case_sensitive ma 0 - czyli nie trzeba wyróżniać dużych i małych liter
                news_word_lst = news_word_lst_lower
            else:
                news_word_lst = news_word_lst_no_change  #jeśli trzeba wyróżniać małe i duże litery > Trakcja i trakcja (dwa inne słowa)

            for i in range(len(news_word_lst)):
                text_to_analyze = news_word_lst[i][1]
                if comp_name in text_to_analyze:
                    upper_check_bool = check_upper_case(company_name=comp_name, text_cleaned=text_to_analyze, text_upper=news_word_lst_no_change[i][1])
                    if_excluded = self.check_excluded_keywords_2(text_to_analyze, ir[compID_f_id])  # duże i małe litery nie mają znaczenia

                    if if_excluded == 0 and upper_check_bool is True:  # excluded jest równe 1 jeśli słowo wykluczające daną spółkę występuje w tytule
                        dict_1 = self.rss_news[self.rss_news['id'] == news_word_lst[i][0]].to_dict('records')[0]
                        dict_1['emitent'] = ir[emitent_id]
                        dict_1['compID'] = ir[compID_f_id]

                        dict_1['counter'] = text_to_analyze.count(comp_name)
                        dict_1['display_text'] = get_display_text(dict_1['summary'][:display_text_chars])  #tekst z usuniętym ostatnim zdaniem oraz uporzadkowanymi \s

                        avg_time, readable_index = text_processing(dict_1['article'])  #obliczanie średniego czasu czytania oraz współczynnika czytelności artykułu
                        dict_1['avg_reading'] = avg_time
                        dict_1['readable_index'] = readable_index

                        if comp_name in text_to_analyze.split('akflhviusfhseiuv')[0]:  #[0] to znaczy że jest w tytule lub desc
                            dict_1['mentioned'] = 1  #jest ważniejszy bo w tytule lub desc
                        else:
                            dict_1['mentioned'] = 0  #jest mniej wazny bo tylko w tekście artykułu

                        dict_1['score'] = calculate_article_score(dict_1)  #zobaczyć czy zadziała
                        frame.append(dict_1)

        df = pd.DataFrame(frame)
        return df

    def check_excluded_keywords_2(self, text, comp_id):  #funkcja do sprawdzania czy w danym tekście nie ma słów wykluczających spółkę - duże i małe litery nie mają znaczenia
        excluded_keywords_list = self.keywords_excluded[self.keywords_excluded['compID_f'] == comp_id]['keyword_excluded'].tolist()  #słowa wykluczające dla danej spółki
        variable = 0

        for keyword in excluded_keywords_list: #dla każdego słowa kluczowego w liście (w tym dwa i więcej słów jak AC Milan)
            keyword = " " + keyword.lower() + " "

            if keyword in text.lower():
                variable = 1  # 1 jeśli zostanie znalezione wyrażenie wykluczające daną spółkę
                break
        return variable

    def news_df_new(self, lower_or_nochange):
        df_temp = self.rss_news.copy()
        df_temp['article_truncated'] = df_temp['article'].apply(lambda x: pd.NA if pd.isnull(x) else truncate_string(x))

        df_temp['new_txt'] = df_temp['title'] + " " + df_temp['summary'] + " akflhviusfhseiuv " + df_temp['article_truncated']
        df_list = list(zip(self.rss_news.id, df_temp['new_txt']))

        df_list_final = []

        for element in df_list:
            text = str(element[1])

            if lower_or_nochange == 0:  # wariant: tytuły z małymi literami
                text_to_analyze = text.lower().translate(str.maketrans('', '', string.punctuation))
            else:  # bez zmian liter tytułów
                text_to_analyze = text.translate(str.maketrans('', '', string.punctuation))

            text_to_analyze = " " + text_to_analyze + " "
            text_to_analyze = re.sub(r"\s+", ' ', text_to_analyze)  # oczyszczanie z wielu white spaces, bo po usunięciu & żeby nie zostały wolne przestrzenie

            nested_list = [element[0], text_to_analyze]
            df_list_final.append(nested_list)
        return df_list_final
