from datetime import datetime
import hashlib
import random
import time
import os

#KONFIGURACJA ZMIENNYCH DO SKRYPTU
from dotenv import dotenv_values

from mysql_db import table_management


#filepath = os.path.abspath("scrapy_variables.env")
#config = dotenv_values(filepath)  #może zadziała na serwerze
config = dotenv_values("/var/www/html/tuinwestor.pl/python_work_files/scrapy_variables.env")


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

date = datetime.now()



def time_lag():
    num = random.choice(range(100, 200))
    sec = num / 100
    time.sleep(sec)


def time_sleep(interval_start, interval_stop):
    start = int(interval_start * 100)
    stop = int(interval_stop * 100)
    num = random.choice(range(start, stop))
    sec = num / 100
    time.sleep(sec)


def hashing_SHA2(string):
    # encode the string
    encoded_str = string.encode()

    # create sha-2 hash objects initialized with the encoded string
    hash_obj_sha224 = hashlib.sha224(encoded_str)  # SHA224\
    #print(hash_obj_sha224.hexdigest())
    return hash_obj_sha224.hexdigest()
