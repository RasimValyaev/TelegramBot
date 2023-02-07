# https://surik00.gitbooks.io/aiogram-lessons/content/
# https://mastergroosha.github.io/aiogram-3-guide/
# https://github.com/noXplode/aiogram_calendar/blob/master/example_bot.py
# Не забывайте своевременно обновлять библиотеку командой: python.exe -m pip install aiogram -U
import datetime
import subprocess
import logging
import asyncio
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import Dispatcher
from aiogram_calendar import simple_cal_callback, SimpleCalendar, dialog_cal_callback, DialogCalendar
from aiogram.dispatcher.filters import Text
from OneC import main_one_c
from PrivatBank import main_privatbank
from UserValidate import main_user_validate, add_to_database as save_message
from configPrestige import TELEGRAM_TOKEN
from Cash.docCashAccRegRecordtype import main_doc_cash_acc_reg_recordtype
from Cash.docCashCorrectionOfRegister import main_doc_cash_correction_of_register
from Cash.docCashMoneyCheck import main_doc_money_check
from Cash.docCashPaymentOrderExpenseDetails import main_doc_cash_order_expense_details
from Cash.docCashPaymentOrderReceiptDetails import main_doc_cash_order_receipt_details
from Cash.docCashPaymentOrderWithdrawalOfFunds import main_doc_cash_payment_order_withdrawal_of_funds
from Cash.docCashPaymentOrderWithdrawalOfFundsDetails import main_doc_cash_payment_order_withdrawal_of_funds_details
from Cash.docCashWarrantExpenseDetails import main_doc_cash_warrant_expense_details
from Cash.docCashWarrantReceiptDetails import main_doc_cash_warrant_receipt_details
from Cash.docCashWarrantReceipt import main_doc_cash_warrant_receipt
from Cash.docCashWarrantExpense import main_doc_cash_warrant_expense
from Cash.docCashMovement import main_doc_cash_movement
from Cash.docCashPaymentOrderReceipt import main_doc_cash_order_receipt
from Cash.docCashPaymentOrderExpense import main_doc_cash_order_expense
from Catalog.catBanks import main_cat_banks
from Catalog.catCashBankAccounts import main_cat_cash_bank_accounts
from Catalog.catCashClause import main_cat_cash_clause
from Catalog.catCounterparties import main_cat_counterparties
from Catalog.catCurrency import main_cat_currencies
from views_pg import main_create_views

idmenu = 0
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


@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply("Only for registered users!\n\nТільки для зареєстрованих  користувачів!",
                        reply_markup=start_kb)


#
# @dp.callback_query_handler(text_contains='menu_')
# async def menu(call: types.CallbackQuery):
#     global idmenu
#     if call.data and call.data.startswith("menu_"):
#         code = call.data[-1:]
#         idmenu = code
#         if code.isdigit():
#             code = int(code)
#             idmenu = code
#
#         if code == 1:
#             await echo(call.message)
#             # await call.message.edit_text('Нажата кнопка остатки', reply_markup=keyboard)
#             await call.message.delete()
#         # if code == 2:
#         #     await call.message.edit_text('Нажата кнопка Программы', reply_markup=keyboard)
#         else:
#             # await bot.answer_callback_query(call.id)
#             await bot.send_message(call.id, "ERROR", reply_markup=start_kb)
#

@dp.message_handler(Text(equals=['kalan'], ignore_case=True))
async def nav_cal_handler(message: Message):
    global idmenu
    idmenu = 1
    await get_sms(message)
    # await message.answer("Please select a date: ", reply_markup=await SimpleCalendar().start_calendar())


# simple calendar usage
@dp.callback_query_handler(simple_cal_callback.filter())
async def process_simple_calendar(callback_query: CallbackQuery, callback_data: dict):
    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        await callback_query.message.answer(
            f'You selected {date.strftime("%d/%m/%Y")}',
            reply_markup=start_kb
        )


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
            # file = r"d:\Prestige\Python\Prestige\Outher\update_prestige_cash.bat"
            # result = subprocess.Popen(file)
            # await load_data_from_1c()
            # if result.wait() == 0:
            sms = "%s\n\n**********banka**********\n" % last_date
            sms += main_privatbank(msg.chat.id)
            sms += "\n\n**********1C**********\n"
            sms += main_one_c()
        elif msg.text == 'gider':
            sms = 'Hizmet şu anda mevcut değil!'

    save_message(msg.chat.id, msg.chat.username, save_to_base, msg.date)
    await msg.answer(sms, reply_markup=start_kb)

    if msg.chat.id != 490323168:
        sms = "Отправлено смс\n%s\n%s\n%s" % (msg.chat.id, msg.chat.username, sms)
        await bot.send_message(490323168, sms, reply_markup=start_kb)


@dp.message_handler()
async def echo(msg: types.Message):
    await get_sms(msg)


@dp.message_handler()
async def echo_message(msg: types.Message):
    await bot.send_message(msg.from_user.id, msg.text)


async def load_data_from_1c():
    main_doc_cash_acc_reg_recordtype()
    main_doc_cash_correction_of_register()
    main_doc_money_check()
    main_doc_cash_order_expense_details()
    main_doc_cash_order_receipt_details()
    main_doc_cash_payment_order_withdrawal_of_funds()
    main_doc_cash_payment_order_withdrawal_of_funds_details()
    main_doc_cash_warrant_expense_details()
    main_doc_cash_warrant_receipt_details()
    main_doc_cash_warrant_receipt()
    main_doc_cash_warrant_expense()
    main_doc_cash_movement()
    main_doc_cash_order_receipt()
    main_doc_cash_order_expense()
    main_cat_banks()
    main_cat_cash_bank_accounts()
    main_cat_cash_clause()
    main_cat_counterparties()
    main_cat_currencies()
    main_create_views()


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
