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

import datetime
import warnings
import subprocess
import logging
import asyncio
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import Dispatcher
from aiogram_calendar import simple_cal_callback, SimpleCalendar, dialog_cal_callback, DialogCalendar
from aiogram.dispatcher.filters import Text
from OneC import main_one_c_cash_rest, get_cash_expenses
from PrivatBank import main_privatbank
from UserValidate import main_user_validate, add_to_database as save
from configPrestige import TELEGRAM_TOKEN
from authorize import con_postgres_psycopg2
from views_pg import main_create_views

conpg = con_postgres_psycopg2()

idmenu = 0
warnings.filterwarnings('ignore')
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)

start_kb = ReplyKeyboardMarkup(resize_keyboard=True, )
start_kb.row('gider', 'kalan')

# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)
keyboard = InlineKeyboardMarkup()
menu_1 = InlineKeyboardButton(text='kalan', callback_data="menu_1")
menu_2 = InlineKeyboardButton(text='hareket', callback_data="menu_2")
keyboard.add(menu_1, menu_2)


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
    if len(message.chat.full_name) == 0:
        username = message.chat.first_name + " " + message.chat.last_name + " (" + message.chat.username + ")"
    else:
        username = message.chat.username

    return username


async def save_message(chatid, message_text, username, date, in_out: bool):
    if in_out:
        msg_text = "Musteriden: %s" % message_text
    else:
        msg_text = "Musteriye: %s" % message_text

    # *** save to database message from telegam user
    save(chatid, username, msg_text, date)


async def send_me(chatid, sms, in_out: bool):
    # in_out: True - message from user, False - message to user
    if in_out:
        sms += "musteriden\n%s\n%s" % (chatid, sms)
    else:
        sms += "musteriye\n%s\n%s" % (chatid, sms)

    # send me a copy of the message sent
    if chatid != 490323168:
        await bot.send_message(490323168, "message ellandi:\n%s" % sms, reply_markup=start_kb)


async def gider(date):
    sms = get_cash_expenses(date)
    return sms


@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    username = user_name(message)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, message.text, True)

    await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!",
                        reply_markup=start_kb)


@dp.message_handler(Text(equals=['kalan'], ignore_case=True))
async def nav_cal_handler(message: Message):
    username = await user_name(message)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, message.text, True)

    if await user_validate(message):
        sms = await kalan(message.chat.id, reply_markup=start_kb)
    else:
        sms = "Вы не зарегистрированы в системе!"

    await save_message(message.chat.id, sms, username, message.date, False)
    await send_me(message.chat.id, sms, False)


@dp.message_handler(Text(equals=['gider'], ignore_case=True))
async def simple_cal_handler(message: Message):
    username = await user_name(message)
    await save_message(message.chat.id, message.text, username, message.date, True)
    await send_me(message.chat.id, message.text, True)

    if await user_validate(message):
        sms = await message.answer("Gider tarihini lutfen secin: ",
                                   reply_markup=await DialogCalendar().start_calendar())
    else:
        sms = "Вы не зарегистрированы в системе!"
        await message.answer(sms, reply_markup=start_kb)

    await save_message(message.chat.id, sms, username, message.date, False)
    await send_me(message.chat.id, sms, False)


@dp.callback_query_handler(dialog_cal_callback.filter())
async def process_dialog_calendar(callback_query: CallbackQuery, callback_data: dict):
    selected, date = await DialogCalendar().process_selection(callback_query, callback_data)

    if selected:
        date = date.strftime("%d/%m/%Y")
        chatid = callback_query.message.chat.id
        msg_text = callback_query.message.text

        if msg_text == 'Kasa tarihini lutfen secin:':
            sms = await kalan(chatid, date)
        if msg_text == 'Gider tarihini lutfen secin:':
            sms = await gider(date)

        sms = "***** %s *****\n%s\n" % (date, sms)

        print("sms", sms)

        return sms


async def kalan(chatid):
    last_date = datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
    await bot.send_message(chatid, "Lütfen bekleyin!\nBiraz zaman alacak!", reply_markup=start_kb)
    file = r"d:\Prestige\Python\TelegramBot\Bat\update_prestige_cash.bat"
    result = subprocess.Popen(file)
    if result.wait() == 0:
        sms = "%s\n\n**********banka**********\n" % last_date
        print(sms, datetime.datetime.now())
        sms += main_privatbank(chatid)
        print(sms, datetime.datetime.now())
        sms += "\n\n**********1C**********\n"
        print(sms, datetime.datetime.now())
        sms += main_one_c_cash_rest()
        print(sms, datetime.datetime.now(), "\n kalan OFF")
        return sms


@dp.message_handler()
async def echo_message(message: types.Message):
    await bot.send_message(message.from_user.id, message.text)


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
