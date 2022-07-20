from mysql_db import table_management
from datetime import timedelta, datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import word_tokenize
import re
import numpy as np
import pandas as pd


from news_companies_additional_code import news_companies_company
from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_content = config['table_news_companies_content']  #treść artykułów, summary..
table_news_companies_company = config['table_news_companies_company']  #newsy wzmianki o spółkach

#zmienne
news_n_days_before = int(config['news_n_days_before'])

news_article_sim = float(config['news_article_sim'])
news_summary_sim = float(config['news_summary_sim'])
news_title_sim = float(config['news_title_sim'])



'''
skrypt do analizowania podobnych artykułów

schemat:
1. algorytm bierze nowe newsy ze spółek z tabeli news_companies
2. dla każdej spółki występującej na liście pobierane są newsy starsze z tabeli news_companies_final za ostatni okres (4 dni?)
3. dla każdego nowego newsu obliczane jest podobieństwo ze starymi newsami, jeśli dany news jest podobny przynajmniej w 30% (50%?) newsów podobnych w jednej grupie to
zostaje on dołączony do tej grupy newsów podobnych. 
4. podobieństwo newsów jest mierzone za pierwsze x słów tesktu (do zweryfikowania)
5. jeżeli news nie jest podobny no to po prostu zostaje dołączony do tabeli
6. na końcu po przeliczeniu newsów podobnych odpadalny jest algorytm odpowiedzialny za segregowanie newsów podobnych w swojej grupie - wg jakości tekstu lub priorytetu mediów
7. jeśli dany news jest zestawieniem wiadomości ze spółek to nie jest on podobny do żadnego innego newsu

myśli:
dodać ograniczenie że news nie może być podobny do newsu starszego niż... (żeby nie tworzył się łańcuszek) (na razie nie robimy)
dodać ograniczenie badanego tesktu - x słów od początku  (done)
wiadomości dnia (zestawienie spółek) nie mogą być podobne do artykułu o spółce (done)

Do rozważenia:
> napisać przykładowe zdania, które mogą świadczyć o danej kategorii newsa - typu: "skrót wiadomości ze spółek giełdowych" lub "wiadomości dnia"
'''


n_days_before = news_n_days_before  #x dni przed datą publikacji newsa


class text_sim:

    def __init__(self, sentence_lst, input_txt, text_type): #lista z treścią artykułów oraz i >> numer porównywalnego artykułu na liście
        self.sentence_lst = sentence_lst
        self.input_txt = input_txt
        self.text_type = text_type

    def tokenizer(self, documents):  # tokenizing words for each s=item in documents, list and change to the lower letter
        res = []
        for doc in documents:
            #temp_lst = []
            elem = re.sub(r'([^\w\s])', '', doc)  # oczyszczanie ze znaków interpunkcyjnych
            # elem = nlp(elem) #ewentualnie dodanie lemmatyzacji - obecnie nie działa przez jakiś bug w bibliotekach - jednak poprawa nie jest znacząca
            # elem = [token.lemma_ for token in elem]

            word_list = [w.lower() for w in word_tokenize(elem)] #tokenizowanie słów oraz zamiana na małe litery
            # if self.text_type == 'summary':
            #     word_list = word_list[:70] #dla podsumowań bierzemy tylko pierwsze 71 słów
            if self.text_type == 'article':
                if len(word_list) > 200:
                    word_list = word_list[:-80]  # dla artykułów odrzucamy ostatnie 100 słów - bo na końcu mogą być reklamówki takie same dla danej gazety

            #for word in word_list:
            #    if word not in stop_words: #usuwanie stop words polskich!
            #temp_lst.append(word)
            elem = ' '.join(word_list)
            #print(elem)
            res.append((elem))
        return res #zwracana lista oczyszczonych zdań

    def sim(self):
        docs = text_sim.tokenizer(self, self.sentence_lst)

        tfidf = TfidfVectorizer().fit_transform(docs) #wektoryzacja słów i obliczanie podobieństwa
        # no need to normalize, since Vectorizer will return normalized tf-idf
        pairwise_similarity = tfidf * tfidf.T #podobieńśtwa par artykułów
        return pairwise_similarity

    def similarity_array(self):
        pairwise_similarity = text_sim.sim(self)
        #print(pairwise_similarity.toarray())
        arr = pairwise_similarity.toarray() #macierz z podobieństwem artykułów względem siebie
        return arr #zwracany jest rząd artykułu dla którego występuje zdanie porównujące

    def get_most_similar_text(self):
        input_idx = self.sentence_lst.index(self.input_txt)

        pairwise_similarity = text_sim.sim(self)
        n, _ = pairwise_similarity.shape
        pairwise_similarity[np.arange(n), np.arange(n)] = -1.0
        sim_max_index = pairwise_similarity[input_idx].argmax()
        sim_max_value = np.max(pairwise_similarity[input_idx])
        return sim_max_index, sim_max_value


class news_sim:
    '''
    wskaźnik podobieństwa obliczany jest za pomocą następującego schematu:
    1. wartościa wejściową powinna być tabela z nowymi newsami z różnymi spółkami
    2. z tabeli news_companies_company oraz news_companies_2 (article, title i summary) pobierane są dane
    dotyczące historycznych artykułów o danej spółce zgormadzonych w bazie - z ostatnich x dni
    3. obliczane jest podobieńśtwo dla newsu - po każdym obliczeniu news jest zapisywany do bazy, a tabela starych newsów aktualizowana
    4.
    '''

    one_sim_article = 0
    multi_sim_article = 0

    def __init__(self, news_rss_v1, the_earliest_news_date):
        news_rss_v1 = news_rss_v1.fillna('missing')
        self.news_rss = news_rss_v1.replace('', 'missing')
        self.the_earliest_news_date = the_earliest_news_date

    def get_old_sim_news(self):
        news_date = self.the_earliest_news_date - timedelta(days=n_days_before)  # wyciągamy wszystkie newsy spółki z tabeli news_companies_final do x dni przed

        news_companies_company = table_news_companies_company
        news_companies = table_news_companies_content
        where_condition = f'date({news_companies_company}.date) >= "{news_date.date()}" AND {news_companies_company}.newsID = {news_companies}.newsID'  # AND {news_companies_company}.new = 0'
        cols_1 = ['id', 'similarity', 'date', 'compID', 'categoryID', 'new', 'hash_company']
        cols_2 = ['summary', 'article', 'title']

        cls = table_management(hostname, dbname, uname, pwd)
        df_news_old = cls.fetch_data_multi_tables(news_companies_company, news_companies, cols_1, cols_2, where_condition)

        cls.close_connection_2()
        df_news_old = df_news_old.fillna('no value')
        df_news_old = df_news_old.replace('', 'no value')
        return df_news_old

    def calculate_sim(self):  #dać wyjątek jeśli nie ma tekstu summary
        print("Obliczanie podobieństwa newsów o spółkach...")
        priority = {'article': news_article_sim, 'summary': news_summary_sim, 'title': news_title_sim}  #takie duze podobieństwo żeby przetestować brak podobnego tytuły - oprócz identycznych

        df_news = self.get_old_sim_news()  # dane historyczne newsów
        df_news_old = df_news[df_news['new'] == 0]

        columns = list(self.news_rss.columns)
        hash_company_ind = columns.index('hash_company') + 1
        date_ind = columns.index('date') + 1
        title_ind = columns.index('title') + 1
        art_ind = columns.index('article') + 1
        summary_ind = columns.index('summary') + 1
        comp_id = columns.index('compID') + 1
        categoryID_id = columns.index('categoryID') + 1


        cls = table_management(hostname, dbname, uname, pwd)

        for ir in self.news_rss.itertuples():
            hash_company = ir[hash_company_ind]
            date = ir[date_ind]
            title = ir[title_ind]
            article = ir[art_ind]
            summary = ir[summary_ind]
            compID = ir[comp_id]
            categoryID = ir[categoryID_id]
            news_company_id = df_news.loc[df_news['hash_company'] == hash_company, 'id']  #id newsu w tabeli - aby zupdatować po obliczeniu sim - id

            row_dict = {'id': news_company_id,
                        'title': title,
                        'article': article,
                        'summary': summary,
                        'date': date,
                        'compID': compID,
                        'categoryID': categoryID,
                        }

            df_news_old_filtered = df_news_old[(df_news_old['compID'] == compID) & (df_news_old['categoryID'] == categoryID) & (df_news_old['date'].dt.date == date.date())]  #zmiana z >= na == bo bierzemy artykuły tylko z dnia wyjścia newsa
            array = self.numpy_sim_array(df_news_old_filtered, priority, row_dict)

            sim_sent_final = self.define_similarity_multi_comparison(array)  # ostatecznie podobny artykuł - jeśli jest)
            row_dict['similarity'] = sim_sent_final

            df_news_old = df_news_old.append(row_dict, ignore_index=True)

            cls.update_value(table_news_companies_company, 'similarity', sim_sent_final, 'hash_company', hash_company)  #niezależnie od tego czy jest czy nie ma podobnego newsa
            cls.update_value(table_news_companies_company, 'new', 0, 'hash_company', hash_company)

        cls.close_connection_2()

        print(f"liczba podobnych artykułów pojedyńczych: {self.one_sim_article}")
        print(f"liczba wielu podobnych artykułów: {self.multi_sim_article}")

    def numpy_sim_array(self, df_news_old, priority, row):  #tworzenie macierzy podobieńśtwa dla różnego typu tekstu - article, summary lub tytuł
        numpy_lst = []

        for key, value in priority.items():  #dla każdego artykułu, title, summary
            if len(df_news_old) == 0:
                sim_sent = 0
                sim_max_value = 0
            else:
                text = row[key]  # potem do przykrócenia do x znaków

                lst2 = df_news_old[key].tolist()
                lst1 = [text]
                text_lst = lst2 + lst1  #lista tekstów do porównania podobieństwa

                cls_sim = text_sim(text_lst, text, key)
                sim_max_index, sim_max_value = cls_sim.get_most_similar_text()  # index i wartość najbardziej podobnego tekstu z listy (może byc kilka?)

                text_max_sim = text_lst[sim_max_index]
                text_row = df_news_old[df_news_old[key] == text_max_sim].iloc[0]  # wiersz najbardziej podobnego zdania - jeśli jest kilka takich samych to bierzemy pierwszy (bez znaczenia i tak)

                sim_sent = self.define_sim_sent(text_row, sim_max_value, value)

            temp_list = [sim_sent, sim_max_value, key]  # tworzymy macierz z indeksem sim_sent, wartością podobieńśtwa, indeksem podobnego zdania i nazwą kolumny porównywanej (key)
            numpy_lst.append(temp_list)

        array = np.array(numpy_lst)
        return array

    def define_sim_sent(self, row_max_sim, sim, value):
        if sim > value:  #jeśli podobieństwo jest większe niż x to wtedy...
            row_sim_sent = row_max_sim['similarity']
            row_sim_id = row_max_sim['id']
            if row_sim_sent == 0:  #jeśli zdanie najbardziej podobne nie ma jeszcze swojego podobnego zdania to..
                sim_sent = row_sim_id  #... to sim_sent dla nowego zdania jest równe id zdania najbardziej podobnego
                self.one_sim_article += 1
            else:
                sim_sent = row_sim_sent  #jeśli zdanie najbardziej podobne jest już podobne do innego to nowe zdanie otrzymuje ten sam id podobieństwa
                self.multi_sim_article += 1
        else:
            sim_sent = 0  #początkowa wartość domyślna dla podobnego zdania to 0
        # print(sim_sent)
        return int(sim_sent)  #sporadycznie pojawia się błąd series object czy coś takiego

    def define_similarity_multi_comparison(self, array):  #macierz 3x2, pierwsza kolumna to sim_sent dla badanego newsa dla tesktu: article, seummary, title
        '''
        macierz 3x2
        > pierwsza kolumna to sim_sent dla badanego newsa dla tesktu: article, summary, title
        > druga kolumna to wartość podobieństwa niezaleznie czy przekroczy ono wymagany próg czy nie

        jeśli dwa lub trzy (col 1) są takie same - sim_sent to wtedy zwracamy je
        jeśli każdy jest rózny to sprawdzamy odpowiednio article - summary - title
        pierwszy który z nich nie będzie się równał 0 zostaje przekazany jako sim_sent
        jeżeli wszystkie są równe 0 to znaczy że artykuł nie jest podobny i zwracamy 0
        '''
        arr = array[:, 0]
        arr = arr.astype(np.int32)

        counts = np.bincount(arr)
        max_sim_sent = counts.argmax()
        reps = np.amax(counts)

        sim_sent = 0

        if max_sim_sent != 0 and reps == 2 or reps == 3:
            #print('1')
            sim_sent = max_sim_sent  #jeśli jakiś index w col 1 powtarza się 2-3 razy i nie jest 0
        elif reps == 3 and max_sim_sent == 0:  #jeśli w col 1 są 3x0 to art nie jest podobny do żadnego innego w bd
            #print('2')
            sim_sent = 0
        else:
            #print('3')
            for elem in arr:
                if elem != 0:  #dla pierwszego indexu który nie będzie równy 0 - patrząc od article do title (priorytetowo)
                    sim_sent = elem
        return sim_sent
