import pandas as pd
import re
import string

from mysql_db import table_management
from news_companies_additional_code import check_word_table
from config import config

pd.set_option('display.max_columns', None)


'''
algorytm słuzy do:

> wyłapywania wyjątków dla newsów i odrzucania ich - typu artykuł wydany przez DM ING (analitycy ING...) 

Odrzucane są newsy w tekście których znalezione zostanie słowo wykluczajace w odpowiedniej odległości (3 słowa włacznie) od wzmianki o spółce
Przykłądowo analitycy ING zostanie odrzucone 

Algorytm updatuje nowe odmiany słów kluczowych w praypadku dodania nowego słowa wykluczającego - w tym celu wykorzystywana jest tabel z odmianami wszystkich 
słów słownika polskiego

W tabeli news_companies_exceptions podajemy tylko podstawową formę słowa dla spółki

założenia:
> lista odmian słów polskich nie jest updatowana - i dlatego te które już znajdują się w bazie są ostateczne
'''


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_exception_wform = config['table_news_exception_wform']  #wyjątki słowne - analityk, ekspert..
table_news_companies_keywords = config['table_news_companies_keywords']  #słowa kluczowe słów
table_news_companies_exception = config['table_news_companies_exception']  #wyjątki dla spółek analityka dla Alior Bank itp..



class news_category:

    def __init__(self, news_rss, comp_exception):
        news_rss_v1 = news_rss.fillna('missing')
        self.news_rss = news_rss_v1.replace('', 'missing')
        self.comp_exception = comp_exception  #dataframe z słowami wykluczającymi dla spółki

    def replace_if_null_and_select_comp(self):  #zmienić nazwę potem jesli tak zostanie
        df = self.news_rss.copy()
        comp_lst = list(set(self.comp_exception['compID']))
        df = df[df['compID'].isin(comp_lst)]  #tylko te które są w wyjątkach
        return df

    def get_word_variance(self):
        cls_0 = table_management(hostname, dbname, uname, pwd)
        word_variance = cls_0.get_columns_data(table_news_exception_wform, '*')  # dataframe ze słowami i odmianami obecnymi w bazie
        cls_0.close_connection_2()
        return word_variance

    def find_rejected(self):
        word_variance = self.get_word_variance()  #lista słów z odmianą w bd
        df = self.replace_if_null_and_select_comp()

        df['new_txt'] = df['title'] + " " + df['summary'] + " " + df['article']
        df_list = list(zip(df.index, df.compID, df['new_txt']))

        rejected_index_lst = []

        for element in df_list:  #dla danego tekstu i spółki
            df_index = element[0]
            compID = element[1]
            text = element[2]

            final_exception_lst = self.get_company_exception(compID, word_variance)  #lista słów których nie moze być obok nazwy spółki
            comp_keywords_lst = self.get_company_variance(compID)  #rózne nazwy spółek już zmienione na małe litery

            cleaned_text = self.clean_text(text)  #zrobić żeby podawać tylko tekst jako zmienna
            check_text = self.find_exceptions(cleaned_text, final_exception_lst, comp_keywords_lst)

            if check_text is True:
                rejected_index_lst.append(df_index)

        df_news = self.news_rss.replace("missing", pd.NA)
        df_rej = df_news[df_news.index.isin(rejected_index_lst)]
        df_cleaned = df_news[~df_news.index.isin(rejected_index_lst)]
        return df_rej, df_cleaned

    def get_company_exception(self, compID, word_variance):
        final_lst_exceptions = []

        comp_word_base_lst = self.comp_exception['keyword'][self.comp_exception['compID'] == compID].tolist()

        for keyword in comp_word_base_lst:  #dla słowa podstawowego w tabeli z companies_exceptions
            variance_lst = word_variance['form'][word_variance['base_word'] == keyword].tolist()  #lista odmian dla danego słowa bazowego
            variance_lst = variance_lst + [keyword]  #dodajemy podstawową formę bo nie ma jej po stronie odmian
            final_lst_exceptions = final_lst_exceptions + variance_lst
        return final_lst_exceptions

    def get_company_variance(self, compID):
        cls_0 = table_management(hostname, dbname, uname, pwd)
        comp_keywords = cls_0.get_columns_data(table_news_companies_keywords, '*')  # dataframe ze słowami i odmianami obecnymi w bazie
        cls_0.close_connection_2()
        comp_keywords_lst = comp_keywords['keyword'][comp_keywords['compID_f'] == compID].tolist()
        res = []
        for keyword in comp_keywords_lst:
            res.append(keyword.lower())
        return res

    def find_exceptions(self, text, exception_lst, comp_keywords_lst):
        num_words_to_take = 3
        check = False

        for name in comp_keywords_lst:
            words_name = name.split(' ')
            lgth = len(words_name)

            for i in range(len(text) - lgth + 1):  # dla takiej długości, bo gdy wyrażenie wykluczające jest 3 częściowe to szukamy 3 słów obok siebie więc max do 3 słowa od końca w liście tytułu
                long = i + lgth
                words_in_title = text[i:long]  # x kolejnych słów w zależności od liczby słów wykluczających

                if words_in_title == words_name:  #jeśli dane słowa są takie same jak słowa wykluczające (jedno wyrażenie) to koniec pętli i wyrzucamy badane zdanie / tekst
                    end = long + num_words_to_take  # końcowy index
                    start = i - num_words_to_take if num_words_to_take <= i else 0  # początkowy index

                    word_lst = text[start:end]  # wyrazy brane pod uwagę wokół nazwy spółki
                    if any(word in word_lst for word in exception_lst):
                        check = True
        return check

    def clean_text(self, text_raw):  #lista z lista odpowiednio (compID, article)
        text_raw = text_raw.lower().translate(str.maketrans('', '', string.punctuation))
        text_white_spaces = re.sub(r"\s+", ' ', text_raw)  # oczyszczanie z wielu white spaces typu \n \r itp..)

        text = text_white_spaces.split(' ')  # rozdzielanie tekstu na słowa
        text_to_analyze = list(filter(str.strip, text))
        return text_to_analyze



def rejected_news(news_rss_v1):  #funkcja do wyszukiwania odrzuconych newsów - tabelą wejściową są nowe newsy z artykułem i spółką
    cls_0 = table_management(hostname, dbname, uname, pwd)
    comp_exception = cls_0.get_columns_data(table_news_companies_exception, '*')  # dataframe z słowami wykluczającymi dla spółki
    cls_0.close_connection_2()

    cls_1 = check_word_table(comp_exception)  #do sprawdzania czy mamy już odmiany słów dostępne w bazie
    cls_1.check_availability()  #ewentualne updatowanie odmiany słów

    cls_2 = news_category(news_rss_v1, comp_exception)
    df_rej, df_cleaned = cls_2.find_rejected()  #zwracany jest dataframe w takiej samej formie jak wejściowy tylko z odrzuconymi newsami
    return df_rej, df_cleaned
