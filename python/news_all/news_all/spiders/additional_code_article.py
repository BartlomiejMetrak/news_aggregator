from newspaper import Article
from bs4 import BeautifulSoup
import hashlib
import re

from ..mysql_db import table_management
from ..settings import config

'''
algorytm do pobierania szczegółowych danych dotyczących artykułów
'''

# zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_news_all = config['table_news_all']
table_news_all_exceptions = config['table_news_all_exceptions']

cls = table_management(hostname, dbname, uname, pwd)
blocked_lst = cls.fetch_all_results_filtered(table_news_all_exceptions, 'string', f'type = 0')
par_lst = cls.fetch_all_results_filtered(table_news_all_exceptions, 'string', f'type = 1')
cls.close_connection_2()  # jeśli else to po prostu zostawiamy starą tabelę

list_of_blocked_articles = [x[0] for x in blocked_lst]  # lista blokowanych artykułów - jeśli są to z reguły przez RODO i Cookies
delete_paragraph = [x[0] for x in par_lst]  # lista usuwanych akapitaów typu > czytaj także itp.


class article_extraction:  # pobieranie dodatkowych danych o artykule typu tekst, opis, summary itp

    def __init__(self, article_link, article_download, image_download):
        self.article_link = article_link  # link do artykułu
        self.article_download = article_download  # czy pobierać artykuł
        self.image_download = image_download  # czy pobierać grafikę do artykułu

    def parse_article(self):
        dict_elem = {
            'link': None,  # self.article_link,
            'image': None,
            'article': None,
        }
        try:  # dla obejścia błędu pobierania artykułu, wtedy brak pobierania i przejście do kolejnego linku
            article = Article('')
            article.download(input_html=self.article_link)
            article.parse()
            article.nlp()
            dict_elem = self.article_data(article, dict_elem)
        except Exception as e:
            print(f"Błąd przy pobieraniu artykułu - przy całej funkcji parse_article: {e}")
        return dict_elem

    def article_data(self, article, dict_elem):
        dict_elem['link'] = article.url

        if self.image_download == 0:  # jeśli 0 to chcemy pobierać - ustawienia w tabeli sources_rss w bd
            try:
                dict_elem['image'] = article.top_image
            except Exception as e:
                print(f"Błąd przy pobieraniu obrazka artykułu: {e}")

        if self.article_download == 0:  # jeśli 0 to chcemy pobierać - ustawienia w tabeli sources_rss w bd
            try:
                article = article.text
                dict_elem['article'] = delete_paragraphs(article)
            except Exception as e:
                print(f"Błąd przy pobieraniu treści artykułu: {e}")
            if any(x in dict_elem['article'] for x in list_of_blocked_articles):
                dict_elem['article'] = None
        return dict_elem


''' edytpwanie artykułu - usuwanie zbędnych paragrafów '''


def delete_paragraphs(article):
    article_cleaned = []
    list_of_paragraphs = article.split('\n')
    for paragraph in list_of_paragraphs:
        if any(x in paragraph for x in delete_paragraph) is False:
            article_cleaned.append(paragraph)
    return '\n'.join(article_cleaned)


''' hashowanie danych '''


def hashing_SHA2(string):
    # encode the string
    encoded_str = string.encode()

    # create sha-2 hash objects initialized with the encoded string
    hash_obj_sha224 = hashlib.sha224(encoded_str)  # SHA224\
    return hash_obj_sha224.hexdigest()


''' pobieranie listy hashów z bazy z ostatnich x dni '''


def get_hash_keys(date_past):
    cls = table_management(hostname, dbname, uname, pwd)
    hash_lst = cls.fetch_all_results_filtered(table_news_all, 'hash_article',
                                              f'date(date) >= "{date_past}"')  # wszystkie artykuły do "date_past" włącznie
    cls.close_connection_2()  # jeśli else to po prostu zostawiamy starą tabelę
    return [x[0] for x in hash_lst]



''' sprawdzanie tekstu artykułu '''

dictionary_source_article_box = {
    1: 'article p',  # wnp
    2: '',  # inwestycje - wszystko jest okej
    3: '.tagdiv-type strong , .tagdiv-type b , .tagdiv-type p, .tagdiv-type li',  # comparic
    4: '#article p',  # bankier
    5: '',  # energwtyka24 - nie pobieramy obecnie newsów - aktualizacja i trudny html (brak rss feed)
    6: 'article p',  # money.pl
    7: '',  # biznesalert - raczej ok
    8: '',  # biznes interia
    9: '.ns-gate-article__content , #npb-a-c p',  # pb.pl
    10: '',  # strefa inwestorów
    11: '',  # computerworld
    12: '#dataContent p',  # papbiznes
    13: '.Article-content strong , .Article-content p , .Article-content li , .Article-content h3',  # css selector - fxmag
    15: '.entry p , .entry h2',  # stockwatch
    16: '.article_heading , .article_p',  # business insider
    17: '.postContent span',  # independent trader
    18: '',
    19: '',
    20: '',
    21: '',
    22: '',
    23: '',
    24: '#penci-post-entry-inner p',  # 300gospodarka
}

dictionary_source_image_box = {  # potencjalna lokalizacja do sprawdzenia obrazka do artykułów
    1: 'div.art-img img',  # wnp
    2: '',  # inwestycje - wszystko jest okej
    3: 'div.td-post-content.tagdiv-type p img',  # comparic
    4: '',  # bankier
    5: '',  # energetyka24 - nie pobieramy obecnie newsów - aktualizacja i trudny html (brak rss feed)
    6: '',  # money.pl
    7: '',  # biznesalert - raczej ok
    8: '',  # biznes interia
    9: '',  # pb.pl
    10: '',  # strefa inwestorów
    11: '',  # computerworld
    12: '',  # papbiznes
    13: '',  # fxmag
    15: '',  # stockwatch
    16: '',  # business insider
    17: '',  # independent trader
    18: '',
    19: '',
    20: '',
    21: '',
    22: '',
    23: '',
    24: '',  # 300gospodarka
}


def get_page_content(response, source_id):
    css_selector = dictionary_source_article_box[source_id]
    if css_selector == '':
        return 'no need to check articles'

    soup = BeautifulSoup(response, "html.parser")

    article_box = soup.select(css_selector)
    article_text = ''
    for box in article_box:
        article_text = article_text + " " + box.text

    article_text = delete_paragraphs(article_text)
    return article_text

def get_image(response, source_id):
    css_selector = dictionary_source_image_box[source_id]
    if css_selector == '':
        return 'no need to check articles'

    soup = BeautifulSoup(response, "html.parser")

    image_box = soup.select(css_selector)
    if image_box:
        if len(image_box) > 0:
            image_url = image_box[0].get('src')
        else:
            return None
    else:
        image_url = None
    return image_url

def clean_text(string_object):
    """ oczyszczanie tekstów artykułu """
    string_object = re.sub(r"\s+", " ", string_object)
    return string_object


class check_article_box:
    """ sprawdzanie wyciągniętego tekstu z tekstem w boxie z artykułem """

    def __init__(self, response, extracted_article, extracted_image, source_id):
        self.response = response  # pobrana treść strony html - przez scrapy
        self.extracted_article = extracted_article  # treść artykułu pobranego przy pomocy newspaper3k
        self.extracted_image = extracted_image  # pobrany obrazek do artykułu
        self.source_id = source_id

    def compare_images(self):
        """ funkcja do porównywania obrazków w niektórych gazetach """
        image_html = get_image(response=self.response, source_id=self.source_id)
        if self.extracted_image is None or image_html is None:
            return None
        elif image_html == 'no need to check articles':
            return self.extracted_image
        else:
            return image_html

    def compare_texts(self):
        """ porównanie wyciągniętych tekstów """
        article_html_box = get_page_content(response=self.response, source_id=self.source_id)
        if self.extracted_article is None or article_html_box is None:
            return None, None, None
        elif article_html_box == 'no need to check articles':
            return None, None, self.extracted_article
        else:
            extracted_cleaned = clean_text(self.extracted_article)
            article_box_cleaned = clean_text(article_html_box)
            extraction_score, words_in_art_perc, article_cleaned = self.find_common_sentences(extracted_cleaned, article_box_cleaned)
            if extraction_score < 0.10 and self.source_id in [3, 4, 9, 13, 15]:  #dla comparic, fxmag, pb i stockwatch
                return extraction_score, words_in_art_perc, article_box_cleaned
            else:
                return extraction_score, words_in_art_perc, article_cleaned

    @staticmethod
    def find_common_sentences(extracted_cleaned, article_box_cleaned):
        """ wyszukiwanie wspólnych tekstów """
        extracted_cleaned_lst = extracted_cleaned.split(' ')
        article_box_cleaned_lst = article_box_cleaned.split(' ')

        new_extracted_lst = []
        out_of_art_words = 0
        in_article_words = 0
        words_in_art = len(article_box_cleaned_lst)

        for word in extracted_cleaned_lst:
            if word in article_box_cleaned_lst:
                first_index = article_box_cleaned_lst.index(word)  # lista z indexami tych słów - na pewno jest jeden element
                article_box_cleaned_lst.pop(first_index)

                new_extracted_lst.append(word)
                in_article_words += 1
            else:
                out_of_art_words += 1

        probability_of_art_extraction = (in_article_words / words_in_art) * (in_article_words / (out_of_art_words + in_article_words))
        new_extracted_text = ' '.join(new_extracted_lst)
        return round(probability_of_art_extraction, 2), round((in_article_words / (out_of_art_words + in_article_words)), 2), new_extracted_text
