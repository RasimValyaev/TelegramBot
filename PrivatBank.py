import sys
import requests
import datetime
import pandas as pd
from datetime import timedelta
from authorize import json_to_dataframe, con_postgres_psycopg2
from configPrestige import dict_to_sql_unqkey
from error_log import add_to_log

dataOt = (datetime.date.today() - timedelta(days=60)).strftime("%d-%m-%Y")
# dataOt = '01-01-2023'
dataDo = (datetime.date.today() + timedelta(days=5)).strftime(
    "%d-%m-%Y")  # не делать до 31/12!!! иначе оплаченные в конце года в интервал НЕ попадут


def send_request_balance(pb_id, pb_token, firm_name, follow_id=''):
    # отправляем POST запрос
    df_total_firm = pd.DataFrame()
    try:
        if follow_id == '':
            url = 'https://acp.privatbank.ua/api/statements/balance/interim?limit=200'
        else:
            url = 'https://acp.privatbank.ua/api/statements/balance/interim' \
                  '?follow_id=%s&limit=200' % follow_id

        headers = {'user-agent': 'Avtoklient', 'id': pb_id, 'token': pb_token,
                   'Content-Type': 'application/json;charset=utf-8'}

        req = requests.post(url, headers=headers)
        if req.status_code in [200, 201]:
            json_data = req.json()  # Выделяем данные из ответа сервера банка
        else:
            sms = 'PrivatBank_api:send_request_balance: Не смог соединиться с серверов Привата. %s' % req.status_code
            print(sms)
            add_to_log(sms)
            sys.exit(0)

        if json_data['status'] != 'ERROR':
            df = parse_json_balance(json_data)  # парсим ответ банка
            df['firm_name'] = firm_name
            # df_total_firm = df_total_firm.append(df, ignore_index=True)
            if df_total_firm.empty:
                df_total_firm = pd.concat([df]).reset_index(drop=True)
            else:
                df_total_firm = pd.concat([df.loc[:]], df_total_firm).reset_index(drop=True)

        if json_data['exist_next_page']:
            # текущая страница не последняя, следовательно переходим на следующую
            follow_id = json_data['next_page_id']
            send_request_balance(pb_id, pb_token, firm_name, follow_id)

    except Exception as e:
        sms = 'PrivatBank_api:send_request_balance: %s' % e
        print(sms)
        add_to_log(sms)

    finally:
        return df_total_firm


def parse_json_balance(json_data):
    k = json_data['balances']
    return json_to_dataframe(k)


def send_request_transaction(pb_id, pb_token, idklienta, follow_id=''):
    # отправляем POST запрос
    try:

        if follow_id == '':
            url = 'https://acp.privatbank.ua/api/statements/transactions/' \
                  '?startDate=%s&endDate=%s&limit=200' % (dataOt, dataDo)
        else:
            url = 'https://acp.privatbank.ua/api/statements/transactions/' \
                  '?startDate=%s&endDate=%s&follow_id=%s&limit=200' % (dataOt, dataDo, follow_id)

        headers = {'user-agent': 'Avtoklient', 'id': pb_id, 'token': pb_token,
                   'Content-Type': 'application/json;charset=utf-8'}

        req = requests.post(url, headers=headers)
        if req.status_code in [200, 201]:
            data = req.json()  # Выделяем данные из ответа сервера банка
        else:
            sms = 'PrivatBank_api:send_request_transaction: Не смог соединиться с серверов Привата. %s' % req.status_code
            print(sms)
            add_to_log(sms)
            sys.exit(0)

        if data['status'] != 'ERROR' and 'next_page_id' in data.keys():
            parse_json_and_add_to_base(data)  # парсим ответ банка

            if data['exist_next_page']:
                # текущая страница не последняя, следовательно переходим на следующую
                follow_id = data['next_page_id']
                send_request_transaction(pb_id, pb_token, idklienta, follow_id)

    except Exception as e:
        sms = 'PrivatBank_api:send_request_transaction: %s' % e
        print(sms)
        add_to_log(sms)


def parse_json_and_add_to_base(json_data):
    # парсим json данные полученные из банка через send_request_transaction
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

                str_sql, odata = dict_to_sql_unqkey('t_pb', data_dict, unqkey)
                # with conpg:
                #     with conpg.cursor() as curpg:
                curpg.execute(str_sql, odata)
                conpg.commit()

            except Exception as e:
                print(str(e))
                conpg.rollback()  # отменяем транзакцию

                if e.args[0] != '23505':
                    sms = 'PrivatBank_api:parse_json_and_add_to_base: %s' % e
                    add_to_log(sms)

                continue

    except Exception as e:
        if 'UniqueViolation' not in e.args[0]:
            print(e.args)
            sms = 'PrivatBank_api:parse_json_and_add_to_base: '
            add_to_log(sms)


def data_authorization_bank(telegram_chatid):
    # данные для авторизации банка
    df = pd.DataFrame()
    try:
        str_sql = '''
          SELECT ids, tokens, ta.isim 
          FROM t_telegram_policy as tp
            LEFT JOIN t_authorize as ta
                ON tp.idfop = ta.id
          WHERE tp.idchat = %s
            and ta.id <> 4
          ORDER BY ta.isim desc
          ;
        ''' % telegram_chatid
        df = pd.read_sql_query(str_sql, con_postgres_psycopg2())
        return df

    except Exception as e:
        sms = 'PrivatBank_api:data_authorization_bank: %s' % e
        print(sms)
        add_to_log(sms)

    finally:
        return df


def firms_cycle_add_to_base(df):
    df_final = pd.DataFrame()
    # цикл по фирмам. заносим движение средств в базу
    for row in df.itertuples(index=False):
        pb_id = row.ids
        pb_token = row.tokens
        firm_name = row.isim
        df_firm = send_request_balance(pb_id, pb_token, firm_name)

        if df_final.empty:
            df_final = df_firm
        else:
            df_final = pd.concat([df_final, df_firm], ignore_index=True)

    return df_final


def convert_df(df_source):
    df = pd.DataFrame()
    try:
        df = pd.DataFrame(df_source, columns=['firm_name', 'currency', 'acc', 'dpd', 'balanceOutEq']).sort_values(
            ['firm_name', 'currency'], ascending=False)
        df = df.reset_index(drop=True)
        df['dpd'] = pd.to_datetime(df['dpd'], format='%d.%m.%Y %H:%M:%S')
        df = df.astype({'balanceOutEq': float})
        df = df[df['balanceOutEq'] != 0]
        df = df.loc[df.groupby(['firm_name', 'currency', 'acc'])['dpd'].idxmax()].reset_index(drop=True)
        df = df.groupby(['firm_name', 'currency'], as_index=False)['balanceOutEq'].sum()

    except Exception as e:
        msg = "ERROR: convert_df %s" % e
        print(msg)
        add_to_log(msg)


    return df


def create_sms(df):
    sms = ''
    for row in df.itertuples():
        firm_name = row.firm_name
        currency = row.currency
        balanceouteq = str("{:.2f}".format(row.balanceOutEq))
        if row.balanceOutEq < 0:
            balanceouteq += ' (EKSI)'

        sms += "\n%s\n%s\n%s\n" % (firm_name, currency, balanceouteq)

    return sms


def main_privatbank(telegram_chatid):
    # Банк. dataframe с данными для авторизации
    data_authorization_bank_df = data_authorization_bank(telegram_chatid)

    # Банк р/с, авторизуемся и заносим данные в базу
    df = firms_cycle_add_to_base(data_authorization_bank_df)
    df = convert_df(df)
    return create_sms(df)



if __name__ == "__main__":
    conpg = con_postgres_psycopg2()
    sms = main_privatbank(490323168, conpg)
    print(sms)
    if conpg:
        conpg.close()
