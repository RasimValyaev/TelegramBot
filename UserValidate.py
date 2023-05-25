# check, is validated user in the telegram_bot
import asyncio
import asyncpg
from async_Postgres import sql_to_dataframe_async, run_sql_pg
from configPrestige import username, psw, basename, hostname, port

SQL_INSERT = '''INSERT INTO t_telegram(chat_id, username, msj, tlgdate) VALUES ($1, $2, $3, $4)'''


async def main_user_validate(telegram_chatid):
    query = "SELECT * FROM t_telegram_policy WHERE idchat = %s"
    df = await sql_to_dataframe_async(query, telegram_chatid)
    return df


async def save_to_database(chatid, user, text, timestamp):
    conn = await asyncpg.connect(user=username, password=psw, database=basename, host=hostname, port=port)
    tr = conn.transaction()
    result = False
    await tr.start()

    try:
        await conn.execute(SQL_INSERT, chatid, user, text, timestamp)

    except Exception as e:
        print(str(e))
        await tr.rollback()
        raise

    else:
        await tr.commit()
        result = True

    finally:
        await conn.close()
        return result


if __name__ == "__main__":
    asyncio.run(main_user_validate(490323161))
    print('OK')
