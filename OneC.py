# loading cash amount from the database

import os
import sys
import pandas as pd

# *********** импортируем данные для подключения к базам
scriptpath = r"d:\Prestige\Python\Config"
sys.path.append(os.path.abspath(scriptpath))
from configPrestige import engine, con_postgres_psycopg2

conpg = con_postgres_psycopg2()


def sql_rest_of_cash():
    return '''
        SELECT currency, kasa, (sum(amount_receipt) + sum(amount_expense)) AS amount
        FROM public.v_one_cach
        WHERE bank_account_cashier_key IN 
            (SELECT DISTINCT 
                bank_account_cashier_key
            FROM public.v_one_cach
            WHERE (doc_date >= '2022-01-01') 
                AND (type_of_cash = 'Наличные')
            GROUP BY bank_account_cashier_key)
        GROUP BY currency, kasa
        ORDER BY currency, kasa;    
    '''


def get_sql(field, table_name, date):
    return '''
        SELECT %s
        FROM %s
        WHERE doc_date = %s
    ''' % (field, table_name, date)

def get_cash_expenses(date):
    sql = get_sql('currency', 'public.v_one_cach', date)
    currencydf = pd.read_sql(sql, conpg)

def create_sms(df):
    sms = ''
    for row in df.itertuples():
        firm_name = row.kasa
        currency = row.currency
        amount = str("{:.2f}".format(row.amount))
        if row.amount < 0:
            amount += ' (EKSI)'

        sms += "\n%s\n%s\n%s\n" % (firm_name, currency, amount)

    return sms


def main_one_c():
    sql = sql_rest_of_cash()
    df = pd.read_sql(sql, conpg)
    sms = create_sms(df)
    return sms


if __name__ == '__main__':
    main_one_c()
