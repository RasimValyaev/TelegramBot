# loading cash amount from the database
import datetime
import os
import sys
import pandas as pd

# *********** импортируем данные для подключения к базам
scriptpath = r"d:\Prestige\Python\Config"
sys.path.append(os.path.abspath(scriptpath))
from authorize import con_postgres_psycopg2

conpg = con_postgres_psycopg2()


def sql_rest_of_cash():
    return '''
        SELECT currency, kasa, (sum(amount_receipt) + sum(amount_expense)) AS amount
        FROM v_one_cach
        WHERE bank_account_cashier_key IN 
            (SELECT DISTINCT 
                bank_account_cashier_key
            FROM v_one_cach
            WHERE (doc_date >= '2022-01-01') 
                AND (type_of_cash = 'Наличные')
            GROUP BY bank_account_cashier_key)
        GROUP BY currency, kasa
        ORDER BY currency, kasa;    
    '''


def create_sql_universal(field, table_name, parameter):
    return '''
        SELECT DISTINCT %s
        FROM %s
        WHERE %s
        ORDER BY %s
    ''' % (field, table_name, parameter, field)


def get_currencies(prm):
    # create sql of type currency
    field = "currency"
    parameter = "doc_date = '%s'" % prm
    table_name = 'v_one_cach'
    sql = create_sql_universal(field, table_name, parameter)
    return sql


def get_cash_expenses(date):
    sms = ''
    parameter_kasa = ''
    parameter_description = ''
    field = "currency"
    parameter_currency = "doc_date = '%s'" % date
    table_name = 'v_one_cach'
    sql = create_sql_universal(field, table_name, parameter_currency)
    currencies = pd.read_sql(sql, conpg)
    for idcurrency, currency in currencies.itertuples():
        field = "kasa"
        parameter_kasa = "%s AND currency = '%s'" % (parameter_currency, currency)
        sql = create_sql_universal(field, table_name, parameter_kasa)
        cash_boxs = pd.read_sql(sql, conpg)
        for id_cash_box, cash_box in cash_boxs.itertuples():
            field = "concat(client,': ', a_comment)" \
                    ", sum(amount_receipt + amount_expense) OVER (PARTITION BY currency, concat(client,': ', a_comment))"
            parameter_description = "%s AND kasa = '%s'" % (parameter_kasa, cash_box)
            sql = create_sql_universal(field, table_name, parameter_description)
            df = pd.read_sql(sql, conpg)
            sms_cur = create_sms_movement(df)
            sms += '\n\n***** %s *****%s' % (cash_box, sms_cur)
            pass
        sms = currency + '\n' + sms

    date = datetime.datetime.strftime(datetime.datetime.now(),"%d.%m.%Y %H:%M:%S")
    sms = date + sms
    return sms


def create_sms_movement(df, sms=''):
    for row in df.itertuples():
        firm_name = row[1]
        amount = str("{:.2f}".format(row[2]))
        sms += "\n%s\n%s\n" % (firm_name, amount)

    return sms


def create_sms_rest(df):
    sms = ''
    for row in df.itertuples():
        firm_name = row.kasa
        currency = row.currency
        amount = str("{:.2f}".format(row.amount))
        if row.amount < 0:
            amount += ' (EKSI)'

        sms += "\n%s\n%s\n%s\n" % (firm_name, currency, amount)

    return sms


def main_one_c_cash_rest():
    # 1С - rest cash in 1C
    conpg = con_postgres_psycopg2()
    sql = sql_rest_of_cash()
    df = pd.read_sql(sql, conpg)
    sms = create_sms_rest(df)
    return sms


if __name__ == '__main__':
    # main_one_c_cash_rest()
    get_cash_expenses('01.02.2023')
