# https://surik00.gitbooks.io/aiogram-lessons/content/
# https://mastergroosha.github.io/aiogram-3-guide/

# Не забывайте своевременно обновлять библиотеку командой: python.exe -m pip install aiogram -U
import logging

import asyncio
from aiogram import Bot, types
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from aiogram.dispatcher import Dispatcher

API_TOKEN = "5728309503:AAFOMRt9xnvNWNEJ6N71n-NLGlKnQvw1UIg"
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)


# @dp.message_handler(commands=['start', 'help'])
# async def send_welcome(message: types.Message):
#     await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!")
@dp.message_handler(commands="start")
async def cmd_start(message: types.Message):
    kb = [
        [
            types.KeyboardButton(text="да"),
            types.KeyboardButton(text="нет")
        ],
    ]
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="отправить номер телефона"
    )
    await message.answer("хотите отправить номер тлф для регистрации?", reply_markup=keyboard)


@dp.message_handler()
async def echo(message: types.Message):
    await message.answer(message.text)


@dp.message_handler(content_types="photo")
async def download_photo(message: types.Message, bot: Bot):
    await bot.download(
        message.photo[-1],
        destination=f"/tmp/{message.photo[-1].file_id}.jpg"
    )


@dp.message_handler(content_types=types.ContentType.STICKER)
async def download_sticker(message: types.Message, bot: Bot):
    await bot.download(
        message.sticker,
        destination=f"/tmp/{message.sticker.file_id}.webp"
    )


# @dp.message_handler()
# async def echo(message: types.Message):
#     await message.reply(message.text)


@dp.message_handler()
async def echo_message(msg: types.Message):
    await bot.send_message(msg.from_user.id, msg.text)


# if __name__ == '__main__':
#     executor.start_polling(dp, skip_updates=True)


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
