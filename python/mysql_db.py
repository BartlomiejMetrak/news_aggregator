import mysql.connector
import pandas as pd
from mysql.connector import FieldType
import sqlalchemy as sql

#KONFIGURACJA ZMIENNYCH DO SKRYPTU
from dotenv import dotenv_values

#filepath = os.path.abspath("scrapy_variables.env")
#config = dotenv_values(filepath)  #może zadziała na serwerze
config = dotenv_values("/var/www/html/tuinwestor.pl/python_work_files/scrapy_variables.env")


#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']



class create_db_or_tab:

    def __init__(self, hostname, dbname, uname, pwd): #podajemy nazwę hosta, nazwę bazy danych, nazwę usera oraz hasło usera
        self.hostname = hostname
        self.dbname = dbname
        self.uname = uname
        self.pwd = pwd

    def create_table(self, table_spec): #"CREATE TABLE espi (espiID int PRIMARY KEY AUTO_INCREMENT, date DATETIME, number VARCHAR(50), company VARCHAR(50), title VARCHAR(5000))"
        db = mysql.connector.connect(
            host=self.hostname,
            user=self.uname,
            passwd=self.pwd,
            database=self.dbname)
        mycursor = db.cursor()  # to be able to curse through database and run sql queries and get data
        mycursor.execute(table_spec)
        db.commit()
        #create_db_or_tab.close_connection(self, mycursor, db)

    def create_table_sqlalchemy(self, new_tabname): #prefer not to use it - function inside needs to be changed then (column names)
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))

        meta = sql.MetaData()
        students = sql.Table(
            new_tabname, meta,
            sql.Column('id', sql.Integer, primary_key=True),
            sql.Column('name', sql.String(128)),
            sql.Column('lastname', sql.Text),
        )
        meta.create_all(engine)

    def create_database(self, new_dbname): #do tworzenia nowej bazy danych - nazwa bazy powinna być jednoczłonowa
        db = mysql.connector.connect(
            host=self.hostname,
            user=self.uname,
            passwd=self.pwd)

        sql = """CREATE DATABASE %s""" % new_dbname # 3x" działa tak samo jak 1x"
        mycursor = db.cursor()  # to be able to curse through database and run sql queries and get data
        mycursor.execute(sql)
        db.commit()
        #create_db_or_tab.close_connection(self, mycursor, db)

    def close_connection(self, cursor, conn): #do zamykania połączeń po każdym odpytaniu bazy
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")


class table_management: #do zarządzania istniejącymi tabelami

    def __init__(self, hostname, dbname, uname, pwd): #podajemy nazwę hosta, nazwę bazy danych, nazwę usera oraz hasło usera
        self.hostname = hostname
        self.dbname = dbname
        self.uname = uname
        self.pwd = pwd

        db = mysql.connector.connect( #automatycznie przy wywołaniu klasy - łączy się ona z bazą danych
            host=self.hostname,
            user=self.uname,
            passwd=self.pwd,
            database=self.dbname)
        mycursor = db.cursor(buffered=True)  # to be able to curse through database and run sql queries and get data, buffered - może mozna to inaczej jakoś jeszcze załatwić?
        self.db = db
        self.mycursor = mycursor

    def add_data_row(self, table_name, values, columns, values_str): #columns and values ex: add_data_row("person", "(name, age)", "('Marek', 13)")
        sql = f"INSERT INTO {table_name} {columns} VALUES {values_str}"  #podajemy (%s, %s ..) dlatego, że w ten sposób mozemy przekazać null values
        self.mycursor.execute(sql, values)
        self.db.commit()

    def insert_df(self, df, table_name, how, insert_idx): #how: 'append' or 'replace' or ... working ex: (df, 'gpw_companies', 'replace', False)
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))
        df.to_sql(table_name, con=engine, if_exists=how, index=insert_idx) #Write DataFrame index as a column. Uses index_label as the column name in the table, default True
        self.db.commit()

    ''' columns get_table_df >> get_columns_data '''
    def get_columns_data(self, table_name, columns): #working ex: ('gaming_alt_data', 'appid, name, publisher, developer')
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))
        df = pd.read_sql(f'SELECT {columns} FROM {table_name}', con=engine) #można dodatkowo dodać: ORDER BY id ASC
        return df #zwracany przefiltrowany dataframe ze wszystkimi kolumnami

    ''' get_multi_filtered_table_df i get_filtered_table_df >> get_multi_filtered_columns_df'''
    def get_multi_filtered_columns_df(self, table_name, columns, condition): #working ex: select_filtered_tab("test", "gender = 'F' OR id < 2")
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))
        df = pd.read_sql(f'SELECT {columns} FROM {table_name} WHERE {condition}', con=engine) #można dodatkowo dodać: ORDER BY id ASC
        return df #zwracany przefiltrowany dataframe ze wszystkimi kolumnami

    def get_custom_filtered_in_list(self, table, cols, filtered_col, lst):  #ex. ('data_file', ['idData', 'source'], 'idData', lst)
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))
        connection = engine.connect()
        metadata = sql.MetaData()
        data_file = sql.Table(table, metadata, autoload=True, autoload_with=engine)

        df_cols = []
        for col in cols:
            df = [data_file.columns[col]]
            df_cols = df_cols + df

        var = sql.select(df_cols).where(data_file.columns[filtered_col].in_(lst))  #pobieramy tylko te które są na liście
        #var = sql.select([sql.func.max(data_file.columns.id)])
        #result = {c.key: getattr(var, c.key) for c in sql.inspect(var).mapper.column_attrs}
        result = connection.execute(var).fetchall()
        return result #zwracany przefiltrowany dataframe ze wszystkimi kolumnami

    def get_custom_filtered_substring_in_list(self, table, cols, filtered_col, reg_string):  #ex. ('data_file', ['idData', 'source'], 'idData', reg_string) >> '(\s*(' + ')\s*)|(\s*('.join(lst) + ')\s*)'
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))
        connection = engine.connect()
        metadata = sql.MetaData()
        data_file = sql.Table(table, metadata, autoload=True, autoload_with=engine)

        df_cols = []
        for col in cols:
            df = [data_file.columns[col]]
            df_cols = df_cols + df

        var = sql.select(df_cols).where(data_file.columns[filtered_col].regexp_match(reg_string))
        result = connection.execute(var).fetchall()
        return result #zwracany przefiltrowany dataframe ze wszystkimi kolumnami

    def get_max_value(self, table, col):
        statement = f"SELECT MAX({col}) FROM {table}"
        self.mycursor.execute(statement)
        frame = self.mycursor.fetchone()  #zakłądamy że maksymalna wartość jest jedna nawet jesli się powatarza kilka razy
        value = frame[0]
        return value

    def last_row_id(self, table_name, ind_name):
        self.mycursor.execute(f"SELECT {ind_name} FROM {table_name} WHERE {ind_name}=(SELECT MAX({ind_name}) FROM {table_name})")
        last_id = self.mycursor.fetchone()
        for lastID in last_id:
            return lastID

    def get_column_names(self, table_name): #return a list of column names in table
        self.mycursor.execute(f"select * from {table_name}")
        columns = [i[0] for i in self.mycursor.description]
        return columns

    def take_last_N(self, table_name, columns, N): #ostatnie N rzędów, zwracany dataframe bez sortowania
        self.mycursor.execute(f"SELECT {columns} FROM {table_name} ORDER BY ind DESC LIMIT {N}")
        frame = self.mycursor.fetchall()

        columns = table_management.get_column_names(self, table_name)
        df = pd.DataFrame(frame, columns=columns)
        return df

    def get_table_desc(self, table_name, columns): #working ex: get_table_desc("person")
        mycursor = self.db.cursor(buffered=True) #dodane buffered żeby móc wykonywać wiele komend za jednym razem - przy wyciąganiu informacji wyjątkowo
        mycursor.execute(f"SELECT {columns} FROM {table_name}") #wybranie tabeli i wszystkich kolumn - select *

        for i in range(len(mycursor.description)):
            print("Column {}:".format(i + 1))
            desc = mycursor.description[i]
            print("column_name = {}".format(desc[0]))
            print("type = {} ({})".format(desc[1], FieldType.get_info(desc[1])))
            print("null_ok = {}".format(desc[6]))
            print("column_flags = {}".format(desc[7]))
            print("-----------------------")

        print("--------- data types per column: ------------\n")
        mycursor.execute("DESC {tab_name}".format(tab_name=table_name))
        for x in mycursor.fetchall():
            print(x)
        #table_management.close_connection(self, mycursor, self.db)

    def fetch_all_results(self, table_name, columns): #pobieranie wszystkich wyników tabeli w formie listy
        self.mycursor.execute(f"select {columns} from {table_name}")
        result = self.mycursor.fetchall()
        return result

    def fetch_all_results_filtered(self, table_name, columns, filt_col_statement): #pobieranie wszystkich wyników tabeli w formie listy
        self.mycursor.execute(f'select {columns} from {table_name} WHERE {filt_col_statement}')
        result = self.mycursor.fetchall()
        return result

    def fetch_one_result_filtered(self, table_name, columns, filt_col_statement): #pobieranie wszystkich wyników tabeli w formie listy
        self.mycursor.execute(f'select {columns} from {table_name} WHERE {filt_col_statement}')
        result = self.mycursor.fetchone()
        return result

    def fetch_data_multi_tables(self, table_1, table_2, cols_1, cols_2, where_condition):
        engine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.hostname, db=self.dbname, user=self.uname, pw=self.pwd))

        string_1 = ''
        for col_1 in cols_1:
            string_1 = string_1 + f'{table_1}.{col_1},'

        for col_2 in cols_2:
            string_1 = string_1 + f'{table_2}.{col_2},'

        sql_query = f""" SELECT {string_1[:-1]} FROM {table_1}, {table_2} WHERE {where_condition}"""
        data = pd.read_sql(sql_query, con=engine) #można dodatkowo dodać: ORDER BY id ASC
        return data


    def update_value(self, table_name, col_name, val, col_condition, value_condition): #zmiana pojedyńczej wartości w tabeli, ex: update_value(table, 'emitent', 'test', 'id', '1')
        sql_update_query = f"UPDATE {table_name} SET {col_name} = '{val}' where {col_condition} = '{value_condition}'"
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def update_values(self, table_name, col_names, vals, col_condition, value_string): #podajemy listy (vals i col_names, warunek i (%s, %s..)
        string_lst = []
        for i in range(len(col_names)):
            col = col_names[i]
            string_lst.append(f'{col}=%s')
        string = ', '.join(string_lst)

        sql_update_query = f"UPDATE {table_name} SET {string} where {col_condition} = {value_string}"
        self.mycursor.execute(sql_update_query, vals)
        self.db.commit()

    def update_values_condition(self, table_name, col_names, vals, condition): #podajemy listy (vals i col_names, warunek i (%s, %s..)
        string_lst = []
        for i in range(len(col_names)):
            col = col_names[i]
            string_lst.append(f'{col}=%s')
        string = ', '.join(string_lst)

        sql_update_query = f"UPDATE {table_name} SET {string} where {condition}"
        self.mycursor.execute(sql_update_query, vals)
        self.db.commit()

    def insert_values(self, table_name, col_names, values): #(name, address)
        sql_update_query = "INSERT INTO {tab} {cols} VALUES {value}".format(tab=table_name, cols=col_names, value=values)
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def delete_rows_condition(self, table_name, condition):
        sql_update_query = f"DELETE FROM {table_name} WHERE {condition}"
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def insert_values_by_row(self, table_name, columns, values):  #(name, address) (%s, %s)
        sql_update_query = (f"INSERT INTO {table_name} {columns} VALUES {values}")
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def set_column_value(self, table_name, column, value):  #('news_companies', 'new', 1)
        sql_update_query = (f"UPDATE {table_name} SET {column} = {value}")
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def reset_autoincrement(self, table_name, value):  #rozpoczynanie stopniowania od danej liczby
        sql_update_query = (f"ALTER TABLE {table_name} AUTO_INCREMENT = {value}")
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def truncate_table(self, table_name):
        sql_update_query = (f"TRUNCATE TABLE {table_name}")
        self.mycursor.execute(sql_update_query)
        self.db.commit()

    def close_connection(self, cursor, conn): #do zamykania połączeń po każdym odpytaniu bazy
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("\nMySQL connection is closed")

    def close_connection_2(self): #do zamykania połączeń po każdym odpytaniu bazy
        if self.db.is_connected():
            self.mycursor.close()
            self.db.close()
