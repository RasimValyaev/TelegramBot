import traceback
import pandas as pd
import psycopg2
import pyodbc
import requests
import sys

from psycopg2 import IntegrityError
from sqlalchemy import create_engine
from error_log import send_sms, add_to_log
from configPrestige import username, psw, hostname, port, basename, url_const, chatid_rasim, data_auth, schema

engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (username, psw, hostname, port, basename))


def url_count(document):
    return url_const + document + "/$count"


def url_details(document):
    return url_const + document + "/?$top=%s&$skip=%s&$format=json"


def url_accumulate(document):
    return url_const + document + "/?$top=%s&$skip=%s&$format=json&$filter=Active eq true"


def url_main(document):
    return url_details(document) + "&$filter=Posted eq true and ОтражатьВУправленческомУчете eq true"


def url_main_select(document):
    return url_main(document) + "&$select=%s"


def url_details_select(document):
    return url_details(document) + "&$select=%s"


# отправляет файл телеграм бот
def send_file(doc, chat_id=chatid_rasim, sms=''):
    try:
        with open(doc, 'rb') as file:
            post_data = {'chat_id': chat_id, "caption": sms}
            post_file = {'document': file}
            r = requests.post('https://api.telegram.org/bot{token}/sendDocument', data=post_data, files=post_file)
            print(r.text)
    except Exception as e:
        sms = "send_file %s" % e
        print(sms)
        add_to_log(sms)


# send GET request to database 1C
def get_response(url):
    result = ''
    try:
        response = requests.get(url, auth=data_auth)
        if (response.status_code < 200) \
                or (response.status_code >= 300):
            sms = "ERROR url: %s. %s" % (url, response.json()['odata.error']['message']['value'])
            print(sms)
            add_to_log(sms)
            send_sms(sms)

        else:
            result = response.json()['value']  # requests.get(url, auth=(c_login, c_psw)).json()['value']

    except Exception as e:
        sms = "ERROR:get_response: %s" % e
        print(sms)
        add_to_log(sms)
        send_sms(sms)

    finally:
        return result


# connection to access database prestige
def con_access_prestige():
    path_to_db = r"c:\Rasim\VB\Inventarization\Inventarization.accdb"
    try:
        user = "admin"
        psw_acc = "hfvpfc15"
        dns = r'''DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;Uid=%s;Pwd=%s;'''
        return pyodbc.connect(dns % (path_to_db, user, psw_acc))

    except Exception as e:
        sms = "ERROR:con_access_prestige:%s, %s" % (path_to_db, e)
        print(sms)
        add_to_log(sms)
        send_sms(sms)
        return ''


# обработчик, для получения из базы одного значения
def get_result_one_column(str_sql, con, odata=''):
    result = ''
    try:
        with con:
            with con.cursor() as cur:
                if odata == '':
                    cur.execute(str_sql)
                else:
                    cur.execute(str_sql, odata)
                result = cur.fetchone()
                result = result[0] if result else ''

    except Exception as e:
        sms = "ERROR:get_result_one_column: %s " % e
        print(sms)
        add_to_log(sms)

    finally:
        return result


def con_postgres_psycopg2():
    conpg = ''

    try:
        conpg = psycopg2.connect(dbname=basename, user=username, password=psw, host=hostname, port=port,
                                 options="-c search_path=public")
        # conpg = psycopg2.connect(dbname=basename, user=username, password=psw, host=hostname, port=port)

    except Exception as e:
        sms = "ERROR:con_postgres_psycopg2: %s" % e
        print(sms)
        add_to_log(sms)
        send_sms(sms)
        return ''

    finally:
        return conpg


def connect_to_pg():
    try:
        conn = psycopg2.connect(host=hostname, database=basename, user=username, password=psw, port=port)

    except psycopg2.OperationalError as e:
        add_to_log(e)
        raise e

    else:
        print('Connected!')
        return conn


# insert, update, delete data in table
def change_data_in_table_bl(str_sql, con, odata='', comment=''):
    """
    :param str_sql:
    :param con:
    :param odata:
    :param comment:
    :return:
    """
    sms = ''
    result = True
    try:
        with con:
            with con.cursor() as cur:

                if odata == '':
                    cur.execute(str_sql)
                else:
                    cur.execute(str_sql, odata)

                con.commit()

    except Exception as e:
        con.rollback()
        sms = str(sys.exc_info()[0])
        if ('23505' not in sms) \
                and ('invalid token' not in sms) \
                and ('ForeignKeyViolation' not in sms) \
                and ('DuplicateTable' not in sms) \
                and ('DuplicateObject' not in sms):
            result = False
            sms = "Ошибка при добавлении/обновлении данных в базу: %s; str_sql: %s; odata: %s" % (e, str_sql, odata)
            add_to_log(sms)

    finally:
        return result


def change_data_in_table_returning(str_sql, con, odata='', comment=''):
    result = ''
    try:
        with con:
            with con.cursor() as cur:

                if odata == '':
                    result = cur.execute(str_sql)
                else:
                    result = cur.execute(str_sql, odata)

                con.commit()
    except IntegrityError as err:
        con.rollback()
        sms = traceback.print_exc()
        add_to_log(sms)


    except Exception as e:
        result = False
        con.rollback()
        if e.pgcode not in ['23503', '23505', '42P07', '42710']:  # double in Unique
            sms = "%s. Ошибка при добавлении/обновлении данных в базу: %s; str_sql: %s; odata: %s" % (
                comment, e, str_sql, odata)
            print(sms)
            add_to_log(sms)
            send_sms(sms)

    finally:
        return result


def read_sql_to_dataframe(sql_str, conn):
    sql_query = pd.read_sql_query(sql_str, conn)
    df = pd.DataFrame(sql_query)  # , columns=['product_id', 'product_name', 'price'])
    return df


def sql_to_dataframe(sql_query, odata=''):
    # conname - тип соединения. Результат запроса переводит в pandas DataFrame
    sms = ''
    df = ''
    try:
        df = pd.DataFrame()
        with engine.connect() as conn:
            if odata == '':
                s_query = pd.read_sql(sql_query, conn)
            else:
                s_query = pd.read_sql(sql_query, conn, params=odata)
            df = pd.DataFrame(s_query)

            # if len(df) > 0:
            #     for x in df.columns:
            #
            #         # убираем лишние пустоты в значениях
            #         if df[x].dtype == "object":
            #             df[x] = df[x].str.strip()

    except Exception as e:
        print(str(e))
        sms = "ERROR:ConnectToBase:dfExtract: %s" % e
        add_to_log(sms)


    finally:
        if "ERROR" in sms:
            add_to_log(sms)
        return df


def pg_sqlalchemy_df_tosql(df, tablename):
    try:

        # работать только через APPEND, replace удаляет ВСЕ записи
        # df.to_sql(name=tablename, con=engine, index=False, if_exists='append', schema="public")

        with engine.connect() as conn:
            # работать только через APPEND, replace удаляет ВСЕ записи!!!
            # df.to_sql(name=tablename, con=conn, index=False, if_exists='append', method='multi',schema="public")
            df.to_sql(name=tablename, con=conn, index=False, if_exists='append',
                      schema="public")  # работать только через APPEND, replace удаляет ВСЕ записи

    except Exception as e:
        sms = "pg_sqlalchemy_df_tosql %s" % e
        print(sms)
        add_to_log(sms)

    finally:
        conn.close()


def pg_sqlalchemy_table_to_df(tablename):
    try:
        df = pd.DataFrame()
        with engine.connect() as conn:
            df = pd.read_sql_table(tablename,
                                   con=conn,
                                   schema=schema
                                   )

    except Exception as e:
        sms = "pg_sqlalchemy_df_tosql %s" % e
        print(sms)
        add_to_log(sms)

    finally:
        conn.close()
        return df


# send a request to the server
def send_request(**kwargs):
    try:
        header = ''
        data_json = ''
        if 'url' in kwargs:
            url = kwargs['url']
        else:
            print('ОТСУТСТВУЕТ ОБЯЗАТЕЛЬНЫЙ ПРАМЕТР url')
            sys.exit(0)

        if 'method' in kwargs:
            method = kwargs['method']
        else:
            print('ОТСУТСТВУЕТ ОБЯЗАТЕЛЬНЫЙ ПРАМЕТР method')
            sys.exit(0)

        if method in ['POST', 'PATCH']:

            if 'data_json' not in kwargs and 're_posted' not in kwargs:  # re_posted - перепроведение
                print("для запросов 'POST'/'PATCH' отсутствует обязательный параметр data_json")
                sys.exit(0)

            else:

                data_json = kwargs['data_json']

                header = {'Accept': 'application/json',
                          'Accept-Charset': 'UTF-8',
                          'User-Agent': 'Fiddler',
                          'Content-Type': 'application/json'
                          }

        elif method != 'GET':
            print("поддерживаются только типы запросов 'POST','PATCH','GET'. Ваш запрос: ", method)
            sys.exit(0)

        if method == 'POST':
            if 're_posted' not in kwargs:
                data_json = kwargs['data_json']

            return requests.post(url, headers=header, json=data_json, auth=data_auth)

        elif method == 'PATCH':
            if 're_posted' not in kwargs:
                data_json = kwargs['data_json']
            return requests.patch(url, headers=header, json=data_json, auth=data_auth)

        elif method == 'GET':
            response = requests.get(url, auth=data_auth).json()['value']

            if len(response) > 0:
                response = response[0].get(list(response[0])[0])
            else:
                response = ''

            return response

    except Exception as e:
        sms = "send_request %s" % e
        print(sms)
        add_to_log(sms)


def json_to_dataframe(json_data):
    df = pd.json_normalize(json_data)
    return df


# control the count of rows in the json response and the count of rows in the table
def get_result_json_and_table(url, table_name):
    """
    :param url: 
    :param table_name:
    """
    response = requests.get(url + "/?$format=json"
                                  "&$inlinecount=allpages"
                                  "&$select=**&$top=1"
                                  "&$filter=Posted eq true and ОтражатьВУправленческомУчете eq true",
                            auth=data_auth)
    if response.status_code == 200:
        count = int(response.json()['odata.count'])
        intable = int(get_result_one_column("SELECT count(*) FROM public.%s" % table_name, con_postgres_psycopg2()))
        if count != intable:
            sms = "Count: %s: json=%s; intable=%s" % (table_name, count, intable)
            # send_sms(sms)
            print(sms)


def json_to_sql(df):
    # creating column list for insertion
    cols = ",".join([str(i) for i in df.columns.tolist()])

    # Insert DataFrame recrds one by one.
    for i, row in df.iterrows():
        sql_insert = "INSERT INTO public.t_one_doc_return_of_goods_from_customers (" + cols + ") VALUES (" + "%s," * (
                len(row) - 1) + "%s)"
        change_data_in_table_returning(sql_insert, con_postgres_psycopg2(), tuple(row),
                                       'main_doc_return_of_goods_from_customers')


if __name__ == "__main__":
    pg_sqlalchemy_table_to_df('t_one_cat_units_classifier')
