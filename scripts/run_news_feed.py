from timeit import default_timer as timer
from datetime import datetime

from newsy.news_all_feed import run_script

from newsy.news_companies import update_rss_news_comp
from newsy.news_company_keywords import updated_keywords
from newsy.news_companies_additional_code import alerts_table
from config import config


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

table_config_table = config['table_config_table']


'''
do zrobienia:
1. alerty z potencjalnie blokowanymi gazetami - jesli np kilka razy w ciągu dnia nie będziemy mogli pobrać żadnego newsa lub coś 
    - zrobione
4. sprawdzić jak działa wyszukiwanie spółek z punctuation typu seco/warrwick - 
    zamian punct w keywordsach i jak jest w edycji tesktu - jest zamiana tesktu na bez punct i małe litery
6. sprawdziwyświetlanie spółki MOL - czy są newsy i czy w tabeli phrase zaszła zmiana z mol na coś innego

5 Mirbud i Budimex - spojerzenie na ywkres - nie działą summary stockwatch
    - zrobione ale sprawdzić jak działa
'''


"""
Sprawdzić jak działa wyszukiwanie nowych keywordsów - przy powtarzajacych się spółkach - 
dodać usuwanie duplikatów wg już obecnych w BD jeśli się powtarzają
"""

start = timer()
date = datetime.now()


''' aktualizowanie keyword list do spółek - updatujemy co drugi dzień '''

#updated_keywords()
# try:
#     updated_keywords()
# except:
#     info = "Błąd przy aktualizacji listy słów kluczowych do spółek - news_companies_keywords - run script"
#     alerts_table(info, date)



if date.hour not in [2, 3, 4, 5]:
    ''' pobieranie wszystkich newsów z rss_feed - nie odpalamy między 2-6 bo sporo wtedy rss_feed ma poprawki '''

    run_script()
    # try:
    #     run_script()
    # except Exception as e:
    #     print(f"error przy pobieraniu newsów {e}")
    #     info = f"Błąd przy pobieraniu wszystkich newsów ze źródeł w bazie - run script, błąd {e}"
    #     alerts_table(info, date)

    # ''' analizowanie wzmianek ze spółek w pobranych newsach '''
    #update_rss_news_comp()
    # try:
    #     update_rss_news_comp()
    # except Exception as e:
    #     print(f"error przy procesowanie newsów {e}")
    #     info = f"Błąd przy analizowaniu wzmianek ze spółek w pobranych newsach - run script, błąd {e}"
    #     alerts_table(info, date)



end = timer()
print("\ncałkowity proces aktualizowania danych trwał: %s " % round((end - start), 2))