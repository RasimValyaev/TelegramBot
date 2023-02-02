# check, is validate user in the telegram_bot

import os
import sys
import traceback
import pandas as pd
import pandas.io.sql as psql

# *********** импортируем данные для подключения к базам
scriptpath = r"d:\Prestige\Python\Config"
sys.path.append(os.path.abspath(scriptpath))
from configPrestige import con_postgres_psycopg2, engine
from error_log import add_to_log

conpg = con_postgres_psycopg2()


def user_policy(telegram_chatid):
    df = pd.DataFrame()
    try:
        query = '''
            SELECT * 
            FROM t_telegram_policy
            WHERE idchat = %s
        ''' % telegram_chatid

        df = pd.read_sql(query, conpg)

    except Exception as e:
        msg = "ERROR: user_policy %s" % e
        print(msg)
        add_to_log(msg)

    finally:
        return df


def main_user_validate(telegram_chatid):
    df = user_policy(telegram_chatid)
    return df


def add_to_database(chatid, username, text, timestamp):
    query = create_sql_code()
    odata = (chatid, username, text, timestamp)
    result = ''
    try:
        with conpg:
            with conpg.cursor() as cur:

                if odata == '':
                    result = cur.execute(query)
                else:
                    result = cur.execute(query, odata)

                conpg.commit()
    except Exception as err:
        conpg.rollback()
        sms = traceback.print_exc()
        add_to_log(sms)

    finally:
        return result


def create_sql_code():
    return '''
        INSERT INTO public.t_telegram(
            chat_id,
            username,
            msj,
            tlgdate
        )
        VALUES (%s, %s, %s, %s)
    '''


if __name__ == "__main__":
    main_user_validate(490323161)
    if conpg:
        conpg.close()

    print('OK')
