# https://vc.ru/newtechaudit/109368-web-parsing-osnovy-na-python
# pip3 install bs4
# pip3 install lxml
# берем курс из сайта minfin.com.ua заносим в базу

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
from datetime import timedelta
from error_log import add_to_log


# ***********

def get_rate(url):
    # парсим страницу продаж usd
    try:
        col = ['kur_alis', 'kur_satis', 'kur_time']
        df = pd.DataFrame(columns=col)
        df_current = pd.DataFrame(columns=col)
        kur_time = None

        r = requests.get(url)  # отправляем HTTP запрос и получаем результат
        soup = BeautifulSoup(r.text, 'html.parser')

        # tables = soup.find_all('div', attrs={'class': 'point-currency-wrapper'}) #Получаем все таблицы с вопросами

        tables = soup.find_all('div', attrs={'class': 'CardWrapper'})  # Получаем все таблицы с вопросами

        i = 0
        for item in tables:
            try:
                i += 1

                phone = \
                item.find_all('div', attrs={'class': 'icons-point'})[0].find_all('a', href=True)[0]['href'].split(" ")[
                    1]
                rate_raw = item.find_all('div', attrs={'class': 'Typography point-currency__rate cardHeadlineL align'})
                if len(rate_raw) == 0:
                    continue

                print(i, phone)
                result = item.find_all('div', attrs={'class': 'Typography point-currency__rate cardHeadlineL align'})[
                    0].text.replace(",", ".").split("/")
                kur_alis, kur_satis = map(float, result)
                rate_day, _kur_time = item.find_all('div', attrs={'class': 'point-interactions'})[0].text.split(" ")

                if rate_day.lower() == "сегодня":
                    day = dt.date.today()
                elif rate_day.lower() == "вчера":
                    day = dt.date.today() - timedelta(days=1)

                kur_time = str(dt.datetime.strptime(str(day) + ' ' + _kur_time, '%Y-%m-%d %H:%M'))
                kur_time = dt.datetime.strptime(kur_time, '%Y-%m-%d %H:%M:%S')

                df_current = pd.DataFrame({'kur_alis': [kur_alis], 'kur_satis': [kur_satis], 'kur_time': [kur_time]})
                df = pd.concat([df, df_current])

            except Exception as e:
                msj = "ParsingSite:dfParsinKur: %s" % e
                add_to_log(msj)

        if kur_time is not None:
            df = df.sort_values('kur_time', ascending=True).tail(5).sort_values('kur_time', ascending=False)
        else:
            pass

        max_time = 0
        if len(df) > 0:
            avg_kur_alis = float("%.2f" % df['kur_alis'].head(5).mean())  # средний курс
            max_kur_alis = float("%.2f" % df['kur_alis'].head(5).max())  # самый высокий курс
            avg_kur_satis = float("%.2f" % df['kur_satis'].head(5).mean())  # средний курс
            max_kur_satis = float("%.2f" % df['kur_satis'].head(5).max())  # самый высокий курс

            max_time = (df['kur_time']).max()

            print('средний/максимальный курс последних 5 значений: ', avg_kur_alis, avg_kur_satis, max_time)

        else:
            avg_kur_alis = 0
            avg_kur_satis = 0

        return avg_kur_alis, avg_kur_satis, max_time


    except Exception as e:
        msj = "ParsingSite:dfParsinKur: %s" % e
        add_to_log(msj)


if __name__ == '__main__':
    kur_alis, kur_satis, time = get_rate("https://minfin.com.ua/currency/auction/usd/buy/kiev/?order=newest")
    print(kur_alis, kur_satis)
