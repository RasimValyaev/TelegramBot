# https://surik00.gitbooks.io/aiogram-lessons/content/
# https://mastergroosha.github.io/aiogram-3-guide/
import datetime
import subprocess
import logging
import asyncio

from aiogram import Bot, \
    types  # Не забывайте своевременно обновлять библиотеку командой: python.exe -m pip install aiogram -U
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import Dispatcher
from OneC import main_one_c
from PrivatBank import main_privatbank
from UserValidate import main_user_validate, add_to_database

API_TOKEN = "5728309503:AAFOMRt9xnvNWNEJ6N71n-NLGlKnQvw1UIg"
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)
idmenu = 0
# @dp.message_handler(commands=['start', 'help'])
# async def send_welcome(message: types.Message):
#     await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!")
# @dp.message_handler(commands="start")
# async def cmd_start(message: types.Message):
#     await message.answer("Only for registered users", reply_markup=keyboard)


keyboard = InlineKeyboardMarkup()
menu_1 = InlineKeyboardButton(text='остатки', callback_data="menu_1")
keyboard.add(menu_1)


# menu_2 = InlineKeyboardButton(text='курс', callback_data="menu_2")
# menu_3 = InlineKeyboardButton(text='О проекте  📌', callback_data="menu_3")
# keyboard.add(menu_1, menu_2, menu_3)


@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!", reply_markup=keyboard)


@dp.callback_query_handler(text_contains='menu_')
async def menu(call: types.CallbackQuery):
    global idmenu
    if call.data and call.data.startswith("menu_"):
        code = call.data[-1:]
        idmenu = code
        if code.isdigit():
            code = int(code)
            idmenu = code

        if code == 1:
            await echo(call.message)
            # await call.message.edit_text('Нажата кнопка остатки', reply_markup=keyboard)
            await call.message.delete()
        # if code == 2:
        #     await call.message.edit_text('Нажата кнопка Программы', reply_markup=keyboard)
        # if code == 3:
        #     await call.message.edit_text('Нажата кнопка О проекте', reply_markup=keyboard)
        else:
            # await bot.answer_callback_query(call.id)
            await bot.send_message(call.id, "ERROR", reply_markup=keyboard)


@dp.message_handler()
async def echo(msg: types.Message):
    global idmenu
    sms = ''
    print(msg.chat.id, msg.text)
    last_date = datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
    df = main_user_validate(msg.chat.id)
    if idmenu != 1:
        save_to_base = msg.text
        sms = "ERROR"
        await msg.answer(sms, reply_markup=keyboard)
    else:
        save_to_base = "нажата кнопка %s" % idmenu
        if len(df) == 0:
            sms = 'Недостаточно прав'
            await msg.answer(sms, reply_markup=keyboard)
        else:
            file = r"d:\Prestige\Python\Prestige\Outher\update_prestige_cash.bat"
            result = subprocess.Popen(file)
            if result.wait() == 0:
                sms = "%s\n\n**********banka**********\n" % last_date
                sms += main_privatbank(msg.chat.id)
                sms += "\n\n**********1C**********\n"
                sms += main_one_c()
                await msg.answer(sms, reply_markup=keyboard)

    add_to_database(msg.chat.id, msg.chat.username, save_to_base, msg.date)

    idmenu = 0
    sms = "Отправлено смс\n%s\n%s\n%s" % (msg.chat.id, msg.chat.username, sms)

    if msg.chat.id != 490323168:
        await bot.send_message(490323168, sms, reply_markup=keyboard)


@dp.message_handler()
async def echo_message(msg: types.Message):
    await bot.send_message(msg.from_user.id, msg.text)


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
