# https://surik00.gitbooks.io/aiogram-lessons/content/
# https://mastergroosha.github.io/aiogram-3-guide/
# https://github.com/noXplode/aiogram_calendar/blob/master/example_bot.py
# Не забывайте своевременно обновлять библиотеку командой: python.exe -m pip install aiogram -U
import os
import sys

scriptpath = r"D:\Prestige\Python\Config"
sys.path.append(os.path.abspath(scriptpath))
scriptpath = r"d:\Prestige\Python\Prestige"
sys.path.append(os.path.abspath(scriptpath))

from stickers import is_stickers
import datetime
import warnings
import subprocess
import asyncio
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram_calendar import dialog_cal_callback, DialogCalendar
from aiogram.dispatcher.filters import Text
from OneC import main_one_c_cash_rest, get_cash_expenses
from PrivatBank import main_privatbank
from UserValidate import main_user_validate, add_to_database as save
from configPrestige import TELEGRAM_TOKEN
from authorize import con_postgres_psycopg2
from views_pg import main_create_views
from CurrentRate import get_rate

conpg = con_postgres_psycopg2()

idmenu = 0
warnings.filterwarnings('ignore')
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)

start_kb = ReplyKeyboardMarkup(resize_keyboard=True, )
start_kb.row('расходы', 'банк', 'курс')  # , 'остаток 1С')
start_kb_lite = ReplyKeyboardMarkup(resize_keyboard=True, )
start_kb_lite.row('курс')


async def user_validate(message):
    main_create_views()

    # control, is this user registered in the database
    df = main_user_validate(message.chat.id)
    if len(df) == 0:
        return False
    else:
        return True


async def user_name(message: Message):
    # get correct username
    if len(message.full_name) != 0:
        result = message.full_name
    elif len(message.username) != 0:
        result = message.username
    else:
        result = "%s; %s" % (message.first_name, message.last_name)

    return result


async def save_message(chatid, message_text, username, date, in_out: bool):
    # in_out: True - message from user(input), False - message to user(output)
    if in_out:
        msg_text = "Musteriden: %s" % message_text
    else:
        msg_text = "Musteriye: %s" % message_text

    # *** save to database message from telegam user
    save(chatid, username, msg_text, date)


async def send_me(chatid, username, sms, in_out: bool):
    # in_out: True - message from user(input), False - message to user(output)
    if in_out:
        sms = "\nMusteriden\n%s\n%s" % (chatid, sms)
    else:
        sms = "\nMusteriye\n%s\n%s" % (chatid, sms)

    # send me a copy of the message sent
    if chatid != 490323168:
        await bot.send_message(490323168, "message ellandi:\n%s\n%s" % (username, sms), reply_markup=start_kb)


async def gider(date):
    sms = get_cash_expenses(date)
    return sms


async def default_ask(chatid):
    sms = "Only for registered users!\n\nТільки для зареєстрованих користувачів!"
    await bot.send_message(chatid, sms, reply_markup=start_kb_lite)


@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, username, message.text, True)
    await default_ask(message.chat.id)


@dp.message_handler(Text(equals=['банк'], ignore_case=True))
async def nav_cal_handler(message: Message):
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, username, message.text, True)

    if await user_validate(message):
        sms = await banka(message.chat.id)
        username = await user_name(message.chat)
        await save_message(message.chat.id, sms, username, message.date, False)
        await bot.send_message(message.chat.id, sms, reply_markup=start_kb)
        await send_me(message.chat.id, username, sms, False)
    else:
        await default_ask(message.chat.id)


@dp.message_handler(Text(equals=['расходы'], ignore_case=True))
async def simple_cal_handler(message: Message):
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, username, message.text, True)

    if await user_validate(message):
        await message.answer("Gider tarihini lutfen secin: ",
                             reply_markup=await DialogCalendar().start_calendar())
    else:
        await default_ask(message.chat.id)


@dp.message_handler(Text(equals=['курс'], ignore_case=True))
async def simple_cal_handler(message: Message):
    url = "https://minfin.com.ua/currency/auction/usd/buy/kiev/?order=newest"
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, username, message.text, True)
    rate_sale, rate_buy, time = get_rate(url)
    sms = "USD\nпокупка %s\nпродажа %s\n\n%s" % (rate_sale, rate_buy, url)
    if await user_validate(message):
        await message.answer(sms, reply_markup=start_kb)
    else:
        await message.answer(sms, reply_markup=start_kb_lite)

    await send_me(message.chat.id, username, sms, False)


@dp.message_handler(Text(equals=['остаток 1С'], ignore_case=True))
async def simple_cal_handler(message: Message):
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, username, message.text, True)

    if await user_validate(message):
        await message.answer("tarihi lutfen secin: ",
                             reply_markup=await DialogCalendar().start_calendar())
    else:
        await default_ask(message.chat.id)


@dp.callback_query_handler(dialog_cal_callback.filter())
async def process_dialog_calendar(callback_query: CallbackQuery, callback_data: dict):
    selected, date = await DialogCalendar().process_selection(callback_query, callback_data)
    if selected:
        date = date.strftime("%d/%m/%Y")
        chatid = callback_query.message.chat.id
        msg_text = callback_query.message.text

        if msg_text == 'tarihi lutfen secin:':
            sms = await rest_cash_one_crest_cash_one_c(chatid)
        if msg_text == 'Gider tarihini lutfen secin:':
            sms = await gider(date)

        sms = "***** %s *****\n%s\n" % (date, sms)

        print("sms", sms)

        username = await user_name(callback_query.message.chat)
        await save_message(chatid, sms, username, date, False)
        await bot.send_message(chatid, sms, reply_markup=start_kb)
        await send_me(chatid, username, sms, False)


async def banka(chatid):
    last_date = datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
    sms = "%s\n" % last_date
    sms += main_privatbank(chatid)
    print(sms, datetime.datetime.now())
    return sms


async def rest_cash_one_crest_cash_one_c(chatid):
    await bot.send_message(chatid, "Lütfen bekleyin!\nBiraz zaman alacak!", reply_markup=start_kb)
    file = r"d:\Prestige\Python\TelegramBot\Bat\update_prestige_cash.bat"
    result = subprocess.Popen(file)
    sms = ''
    if result.wait() == 0:
        sms = "\n\n**********1C**********\n"
        print(sms, datetime.datetime.now())
        sms += main_one_c_cash_rest()
        print(sms, datetime.datetime.now(), "\n banka OFF")
    return sms


@dp.message_handler()
async def echo_message(message: types.Message):
    username = await user_name(message.chat)
    await save_message(message.chat.id, message.text, username, message.date, False)
    await send_me(message.chat.id, username, message.text, False)

    result_for_day = await is_stickers(message)

    if await user_validate(message) and result_for_day == []:
        await bot.send_message(message.from_user.id, 'ERROR', reply_markup=start_kb)
    else:
        if result_for_day != []:
            date = result_for_day[0]
            quantity = result_for_day[1]
            amount = result_for_day[2]
            sms = "за день %s\nколичество: %s ед\nсумма: %s грн" % (date, quantity, amount)
            await bot.send_message(message.chat.id, sms, reply_markup=start_kb_lite)
        else:
            await default_ask(message.chat.id)


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())