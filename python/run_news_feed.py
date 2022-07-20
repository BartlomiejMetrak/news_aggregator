from timeit import default_timer as timer
from datetime import datetime

from mysql_db import table_management
from news_all_feed import run_script

from news_companies import update_rss_news_comp
from news_company_keywords import update_keywords, updated_config, updated_config_2, updated_keywords
from news_companies_additional_code import alerts_table
from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_config_table = config['table_config_table']


start = timer()
date = datetime.now()


''' aktualizowanie keyword list do spółek - updatujemy co drugi dzień '''

print("Uruchamianie skryptu")

#updated_keywords()
try:
    updated_keywords()
except Exception as e:
    info = f"Błąd przy aktualizacji listy słów kluczowych do spółek - news_companies_keywords - run script, error: {e}"
    alerts_table(info, date)



if date.hour not in [2, 3, 4, 5]:
    ''' pobieranie wszystkich newsów z rss_feed - nie odpalamy między 2-6 bo sporo wtedy rss_feed ma poprawki '''

    #run_script()
    try:
        run_script()
    except Exception as e:
        print(f"error przy pobieraniu newsów {e}")
        info = f"Błąd przy pobieraniu wszystkich newsów ze źródeł w bazie - run script, błąd {e}"
        alerts_table(info, date)

    ''' analizowanie wzmianek ze spółek w pobranych newsach '''
    # update_rss_news_comp()
    try:
        update_rss_news_comp()
    except Exception as e:
        print(f"error przy procesowanie newsów {e}")
        info = f"Błąd przy analizowaniu wzmianek ze spółek w pobranych newsach - run script, błąd {e}"
        alerts_table(info, date)

end = timer()
print("\ncałkowity proces aktualizowania danych trwał: %s \n" % round((end - start), 2))



# import spacy
#
# nlp = spacy.load('pl_core_news_sm', exclude=["tok2vec", "tagger", "parser", "attribute_ruler", "vocab", "morphologizer", "lemmatizer"])
#
# text = "Prezydent poleciał do Kopenhagi w Daniii."
#
# doc = nlp(text)
# names = [(X.text, X.label_) for X in doc.ents]
# print(names)
#
# #nlp = spacy.load('pl_core_news_sm')
#
# print("Worked!")


#/home/andrii/.local/lib/python3.8/site-packages/pl_core_news_sm/pl_core_news_sm-3.2.0
