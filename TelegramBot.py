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
from UserValidate import main_user_validate, add_to_database as save_message
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


async def kalan(chatid, date):
    # last_date = datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
    await bot.send_message(chatid, "Lütfen bekleyin!\nBiraz zaman alacak!", reply_markup=start_kb)
    file = r"d:\Prestige\Python\TelegramBot\Bat\update_prestige_cash.bat"
    result = subprocess.Popen(file)
    if result.wait() == 0:
        main_create_views()
        sms = "%s\n\n**********banka**********\n" % date
        print(sms, datetime.datetime.now())
        sms += main_privatbank(chatid)
        print(sms, datetime.datetime.now())
        sms += "\n\n**********1C**********\n"
        print(sms, datetime.datetime.now())
        sms += main_one_c_cash_rest()
        print(sms, datetime.datetime.now())
        return sms


async def gider(date):
    # date = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y")
    sms = get_cash_expenses(date)
    return sms
    # await bot.send_message(chatid, sms, reply_markup=start_kb)


@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!",
                        reply_markup=start_kb)


@dp.message_handler(Text(equals=['kalan'], ignore_case=True))
async def nav_cal_handler(msg: Message):
    if user_validate(msg.chat.id):
        await msg.answer("Kasa tarihini lutfen secin: ", reply_markup=await DialogCalendar().start_calendar())
    else:
        await msg.answer("Вы не зарегистрированы в системе!")


@dp.message_handler(Text(equals=['gider'], ignore_case=True))
async def simple_cal_handler(msg: Message):
    if user_validate(msg.chat.id):
        await msg.answer("Gider tarihini lutfen secin: ", reply_markup=await DialogCalendar().start_calendar())
    else:
        await msg.answer("Вы не зарегистрированы в системе!")


async def user_validate(chatid):
    df = main_user_validate(chatid)
    if len(df) == 0:
        return False
    else:
        return True


@dp.message_handler()
async def get_sms(msg: types.Message):
    global idmenu
    sms = ''
    save_to_base = msg.text
    print(msg.chat.id, msg.text)
    last_date = datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
    df = main_user_validate(msg.chat.id)
    if len(df) == 0:
        sms = 'Недостаточно прав'
        await msg.answer(sms)
    else:
        if msg.text not in start_kb.keyboard[0]:
            sms = "ERROR"
        elif msg.text == 'kalan':
            await msg.answer("Lütfen bekleyin!\nBiraz zaman alacak!", reply_markup=start_kb)
            file = r"d:\Prestige\Python\TelegramBot\Bat\update_prestige_cash.bat"
            result = subprocess.Popen(file)
            if result.wait() == 0:
                main_create_views()
                sms = "%s\n\n**********banka**********\n" % last_date
                print(sms, datetime.datetime.now())
                sms += main_privatbank(msg.chat.id)
                print(sms, datetime.datetime.now())
                sms += "\n\n**********1C**********\n"
                print(sms, datetime.datetime.now())
                sms += main_one_c_cash_rest()
                print(sms, datetime.datetime.now())
        elif msg.text == 'gider':
            # date = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y")
            # sms = get_cash_expenses(date)
            # date = await msg.answer("Lütfen tarihi secin: ", reply_markup=await DialogCalendar().start_calendar())
            date = simple_cal_handler(msg)
            print(date)

    save_message(msg.chat.id, msg.chat.username, save_to_base, msg.date)
    await msg.answer(sms, reply_markup=start_kb)

    if msg.chat.id != 490323168:
        sms = "Отправлено смс\n%s\n%s\n%s" % (msg.chat.id, msg.chat.username, sms)
        await bot.send_message(490323168, sms, reply_markup=start_kb)


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

        sms = "***** %s \n%s\n" % (date, sms)
        await callback_query.message.answer(sms, reply_markup=start_kb)


@dp.message_handler()
async def echo(msg: types.Message):
    await get_sms(msg)


@dp.message_handler()
async def echo_message(msg: types.Message):
    await bot.send_message(msg.from_user.id, msg.text)


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
