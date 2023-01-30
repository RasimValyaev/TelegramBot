import os
import sys
import requests
import datetime
from datetime import timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "\Config")

# *********** импортируем данные для подключения к базам
scriptpath = r"d:\Prestige\Python\Config"
sys.path.append(os.path.abspath(scriptpath))
from configPrestige import con_postgres_psycopg2, dict_to_sql_unqkey, read_sql_to_dataframe, change_data_in_table_bl
from error_log import add_to_log

conpg = con_postgres_psycopg2()
conpg.set_client_encoding('UNICODE')

dataOt = (datetime.date.today() - timedelta(days=60)).strftime("%d-%m-%Y")
dataOt = '01-01-2019'
dataDo = (datetime.date.today() + timedelta(days=5)).strftime(
    "%d-%m-%Y")  # не делать до 31/12!!! иначе оплаченные в конце года в интервал НЕ попадут


def send_request(pb_id, pb_token, idklienta, followId=''):
    # отправляем POST запрос
    try:

        if followId == '':
            url = 'https://acp.privatbank.ua/api/statements/transactions/' \
                  '?startDate=%s&endDate=%s&limit=200' % (dataOt, dataDo)
        else:
            url = 'https://acp.privatbank.ua/api/statements/transactions/' \
                  '?startDate=%s&endDate=%s&followId=%s&limit=200' % (dataOt, dataDo, followId)

        headers = {'user-agent': 'Avtoklient', 'id': pb_id, 'token': pb_token,
                   'Content-Type': 'application/json;charset=utf-8'}

        req = requests.post(url, headers=headers)
        if req.status_code in [200, 201]:
            data = req.json()  # Выделяем данные из ответа сервера банка
        else:
            msj = 'PrivatBank_api:send_request: Не смог соединиться с серверов Привата. %s' % req.status_code
            print(msj)
            add_to_log(msj)
            sys.exit(0)

        if data['status'] != 'ERROR' and 'next_page_id' in data.keys():

            parse_json_and_add_to_base(data)  # парсим ответ банка

            if data['exist_next_page']:
                # текущая страница не последняя, следовательно переходим на следующую

                followId = data['next_page_id']

                send_request(pb_id, pb_token, idklienta, followId)

        else:
            print('send_request: PrivatBank_Api отсутствует параметр next_page_id. idklienta: %s' % idklienta)

    except Exception as e:
        msj = 'PrivatBank_api:send_request: %s' % e
        print(msj)
        add_to_log(msj)


def parse_json_and_add_to_base(json_data):
    # парсим json данные полученные из банка через send_request
    # и сохраняем в базе postgresql

    try:

        k = json_data['transactions']

        for i in range(len(k)):
            try:
                data_dict = k[i]

                ref = k[i]['REF']
                refn = k[i]['REFN']
                trantype = k[i]['TRANTYPE']
                unqkey = str(trantype) + '_' + str(ref) + str(refn)
                data_dict["unqkey"] = unqkey

                str_sql, odata = dict_to_sql_unqkey('public.t_pb', data_dict, unqkey)
                with conpg:
                    with conpg.cursor() as curpg:
                        curpg.execute(str_sql, odata)
                        conpg.commit()

            except Exception as e:
                print(str(e))
                conpg.rollback()  # отменяем транзакцию

                if e.args[0] != '23505':
                    msj = 'PrivatBank_api:parse_json_and_add_to_base:i: '
                    add_to_log(msj)

                continue

    except Exception as e:
        if 'UniqueViolation' not in e.args[0]:
            print(e.args)
            msj = 'PrivatBank_api:parse_json_and_add_to_base: '
            add_to_log(msj)


def data_authorization_bank():
    # данные для авторизации банка
    df = ''
    try:
        str_sql = '''
          SELECT ids, tokens, ta.isim 
          FROM t_telegram_policy as tp
            LEFT JOIN t_authorize as ta
                ON tp.idfop = ta.id
          WHERE ta.ids is not null 
            and ta.ids != ''
            and ta.id != 4
          ORDER BY ta.isim desc
          ;
        '''

        df = read_sql_to_dataframe(str_sql, conpg)
        return df

    except Exception as e:
        msj = 'PrivatBank_api:data_authorization_bank: %s' % e
        add_to_log(msj)

    finally:
        return df


def firms_cycle_add_to_base(df):
    # цикл по фирмам. заносим движение средств в базу
    for index, row in df.iterrows():
        pb_id = row.ids
        pb_token = row.tokens
        idklienta = row.isim

        print(row.isim)

        send_request(pb_id, pb_token, idklienta)


if __name__ == "__main__":

    try:
        conpg.autocommit = True

        # ******************* этап 2

        # Банк. dataframe с данными для авторизации
        data_authorization_bank_df = data_authorization_bank()

        # Банк р/с, авторизуемся и заносим данные в базу
        firms_cycle_add_to_base(data_authorization_bank_df)


    except Exception as e:
        add_to_log('PrivatBank_api:main: %s' % e)


    finally:
        # закрываем соединения со всеми базами

        if conpg:
            conpg.close()

        print('OK')
