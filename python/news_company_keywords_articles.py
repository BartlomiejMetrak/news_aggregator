import string
import spacy
import re

from mysql_db import table_management
from config import config

""" tutaj pobierane sa historyczne artykuły zebrane w bazie dla danej spółki """


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_companies_company = config['table_news_companies_company']  #newsy wzmianki o spółkach
table_news_companies_content = config['table_news_companies_content']  #treść artykułów, summary..
table_companies = config['table_companies']
table_news_companies_keywords = config['table_news_companies_keywords']  #słowa kluczowe słów


nlp = spacy.load('pl_core_news_sm', exclude=["tok2vec", "tagger", "parser", "attribute_ruler", "vocab", "morphologizer", "lemmatizer"])


def grammar(comp):
    """
    określenie podstawy dla każdego słowa w nazwie spółki
    1: słowo jest dłuższe niż 3 znaki
    0: jeśli słowo jest krótsze niż 3 znaki
    :param comp: nazwa spółki - search_name - jak PKN Orlen
    :return: zwracana lista ze słowa, przykł: [('pkn', 0), ('orl', 1)]
    """
    words = comp.split(' ')  #splitujemy bo są takie spółki jak PKN Orlen lub ABS investments
    base_word_lst = []

    for word in words:
        if word == '' or word == ' ': #jeśli trafią się podwójne spacje itp
            continue
        elif len(word) > 3: #zakładamy że słowa krótsze niż 4 litery są nieodmienialne
            base = word[:-2]
            var = (base.lower(), 1) #1 dla obciętych słów kluczowych - do formy podstawowej
            base_word_lst.append(var)
        else:
            var = (word.lower(), 0) #dla słów krótszych niż 4 litery zakłdamy że nie można ich odmienić 'ABS' - wtedy byłoby za dużo możliwości tylu ABU, ALE itp
            base_word_lst.append(var)
    return base_word_lst

def join_columns(row):
    """
    Do łączenia artykułu, tytułu i summary w jeden tekst do wyszukiwania entities (spacy)
    :param row:
    :return:
    """
    article = row['article'] if row['article'] is not None else ''
    title = row['title'] if row['title'] is not None else ''
    summary = row['summary'] if row['summary'] is not None else ''
    return article + ' ' + summary + ' ' + title

def edit_enitity_str(string_object):
    """ edytowanie wszystkich potencjalnych odmian spółek - znalezionych w tekstach artykułów """
    punctuations = string.punctuation + '…'   #dodajemy jeden wyjątek dla punct
    company_search = string_object.translate(str.maketrans('', '', punctuations))
    company_search = re.sub(r"\s+", ' ', company_search)
    return company_search.lower().strip()

def get_all_keywords_by_compID(comp_id):
    """ pobieranie wszystkich keywordsów dla danej spółki aby potem odfiltrować i nie wgrywać duplikatów """
    cls = table_management(hostname, dbname, uname, pwd)
    keywords_comp = cls.fetch_all_results_filtered(table_news_companies_keywords, 'keyword', f'compID_f = {comp_id}')
    cls.close_connection_2()
    keywords_comp = [x[0] for x in keywords_comp]
    return keywords_comp


class get_articles:

    def __init__(self, compID):
        self.compID = compID
        self.keywords_by_compID = get_all_keywords_by_compID(compID)

    def get_articles_from_DB(self):
        """
        artykuły z bazy danych - społka i treść artykułu
        :return:
        """
        cls = table_management(hostname, dbname, uname, pwd)
        news_companies_company = table_news_companies_company
        news_companies = table_news_companies_content
        where_condition = f'{news_companies_company}.newsID = {news_companies}.newsID AND {news_companies_company}.compID = {self.compID}'
        cols_1 = ['newsID', 'compID', 'emitent']
        cols_2 = ['article', 'summary', 'title', 'link']
        df = cls.fetch_data_multi_tables(table_news_companies_company, table_news_companies_content, cols_1, cols_2, where_condition)
        cls.close_connection_2()
        return df

    def get_ent_list(self):
        """
        Funkcja do wyszukiwania w przykłądowych tekstach o spółkach organizacji i person (spacy)
        Njapierw wyszukujemy wszystkie entyti (persname i orgname), potem jest oczyszczany w func edit_enitity_str(), a
        następnie sprawdzamy czy słowa są równe z formami bazowymi search_name
        :df:        dataframe z danymi do analizy - artykułami, summary, title itp
        :return:    zwracana jest lista z nazwami organizacji i osób
        """
        df = self.get_articles_from_DB()
        if len(df) == 0:
            return []

        df['full_text'] = df.apply(lambda row: join_columns(row), axis=1)
        articles = df['full_text'].tolist()

        ent_lst = []
        for article in articles:
            text = article
            doc = nlp(text)
            names = [(X.text, X.label_) for X in doc.ents]

            for elem in names:
                if elem[1] == 'orgName' or elem[1] == 'persName':
                    if elem[0] not in ent_lst:
                        ent_lst.append(elem[0].lower())
        ent_lst = [edit_enitity_str(x) for x in ent_lst]
        return ent_lst

    def get_comp_info(self):
        cls = table_management(hostname, dbname, uname, pwd)
        comp_info = cls.fetch_one_result_filtered(table_companies, 'name_search,emitent', f'id = {self.compID}')
        cls.close_connection_2()

        search_name_clean = comp_info[0].strip().lower() if comp_info[0] is not None else comp_info[0]

        search_name, emitent = search_name_clean, comp_info[1]
        return search_name, emitent

    def new_words(self):
        """ funkcja wyszukuje w entity nazwy bazowe spółek i sprawdza czy są jakieś odmiany w podanej liście entities (spacy)
            jeśli sa to aktualizowana jest baza keywordsów
        """
        search_name, emitent = self.get_comp_info()
        ent_lst = self.get_ent_list()
        if len(ent_lst) == 0:  #nie ma żadnych entities lub nie ma artykułów
            return None

        base_word_lst = grammar(search_name)  #małe litery bez interpunkcji
        ent_lst_found = []
        for entity in ent_lst: #dla każdej nazwy
            entity_word_lst = entity.split(' ') #rozbijamy każde potencjalne zestawy słów na pojedyncze słowa

            ent_temp_lst = []
            for word in base_word_lst: #dla słowa w nazwie spółki
                if len(word[0]) < 4 and word[1] == 0: #dla słów krótszych niż 4 litery i nie skróconych do formy podstawowej '0'
                    if word[0] in entity_word_lst:
                        ent_temp_lst.append((word[0]))
                else:
                    length = len(word[0])
                    for word_2 in entity_word_lst: #dla każdego słowa w potencjalnej nazwie spółki
                        entity_base = word_2[:length] #bierzemy bazę z potencjalnych nazw spółki z artykułów
                        if entity_base == word[0] and len(word_2) < (length + 5): #dla takich odmian jak mostastal == mostostalowi
                            ent_temp_lst.append(word_2)
            comp_name = ' '.join(ent_temp_lst)
            ent_lst_found.append(comp_name)

        res = [search_name.lower()]
        for ent in ent_lst_found:
            if len(ent) > 0 and ent not in res:  #większe od zera bo mogą być również puste listy
                ent_cleaned = edit_enitity_str(ent)
                res.append(ent_cleaned)
        result = self.remove_punc(res)  # końcowa i oczyszczona lista wariancji odmian nazwy spółki

        result = [x for x in result if x not in self.keywords_by_compID]
        update_keywords_DB(result, emitent, self.compID)  #aktualizowanie danych

    def remove_punc(self, ent_lst):
        """ usuwanie ododatkowych znaków typu \n itp z keywordsów """
        new_lst = []

        for ent in ent_lst:
            ent = re.sub("\n", '', ent)  #usuwanie '\n' z tekstu
            if ent not in new_lst: #usuwanie duplikatów z listy
                new_lst.append(ent)
        return new_lst


''' updatowanie keywordsów z listy do bazy '''

def update_keywords_DB(keywords_list, emitent, compID):
    cls_2 = table_management(hostname, dbname, uname, pwd)  # table_name, col_name, val, col_condition, value_condition)

    columns = ['compID_f', 'keyword', 'emitent', 'new_keyword', 'to_be_updated']  #kolumny w bazie
    col_names_string = "(" + ",".join([str(i) for i in columns]) + ")"
    values_string = "(" + ", ".join(["%s"] * len(columns)) + ")"

    print(f"Nowe keywordsy spółek do dodania do tabeli news_companies_keywords w średnim lub długim terminie:")
    for keyword in keywords_list:
        dict = {
            'compID_f': compID,
            'keyword': keyword,
            'emitent': emitent,
            'new_keyword': 1,
            'to_be_updated': 0,
        }
        data = list(dict.values())
        print(dict)
        cls_2.add_data_row(table_news_companies_keywords, data, col_names_string, values_string)
    cls_2.close_connection_2()

