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
        col = ['rate_buying', 'rate_selling', 'rate_time']
        df = pd.DataFrame(columns=col)
        df_current = pd.DataFrame(columns=col)
        rate_time = None

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

                result = item.find_all('div', attrs={'class': 'Typography point-currency__rate cardHeadlineL align'})[
                    0].text
                result =result.replace(",", ".")
                if "-- --" in result:
                    result =result.replace("-- --",'0')
                    continue

                result =result.split("/")
                rate_buying, rate_selling = map(float, result)
                rate_day, _rate_time = item.find_all('div', attrs={'class': 'point-interactions'})[0].text.split(" ")

                day = dt.date.today()

                if rate_day.lower() == "вчера":
                    day = dt.date.today() - timedelta(days=1)

                rate_time = str(dt.datetime.strptime(str(day) + ' ' + _rate_time, '%Y-%m-%d %H:%M'))
                rate_time = dt.datetime.strptime(rate_time, '%Y-%m-%d %H:%M:%S')

                df_current = pd.DataFrame({'rate_buying': [rate_buying], 'rate_selling': [rate_selling], 'rate_time': [rate_time]})
                df = pd.concat([df, df_current])

            except Exception as e:
                msj = "ParsingSite:dfParsinKur: %s" % e
                add_to_log(msj)

        if rate_time is not None:
            df = df.sort_values('rate_time', ascending=True).tail(5).sort_values('rate_time', ascending=False)
        else:
            pass

        max_time = 0
        if len(df) > 0:
            avg_rate_buying = float("%.2f" % df['rate_buying'].head(5).mean())  # средний курс
            max_rate_buying = float("%.2f" % df['rate_buying'].head(5).max())  # самый высокий курс
            avg_rate_selling = float("%.2f" % df['rate_selling'].head(5).mean())  # средний курс
            max_rate_selling = float("%.2f" % df['rate_selling'].head(5).max())  # самый высокий курс

            max_time = (df['rate_time']).max()

            print('средний/максимальный курс последних 5 значений: ', avg_rate_buying, avg_rate_selling, max_time)

        else:
            avg_rate_buying = 0
            avg_rate_selling = 0

        return avg_rate_buying, avg_rate_selling, max_time


    except Exception as e:
        msj = "ParsingSite:dfParsinKur: %s" % e
        add_to_log(msj)


if __name__ == '__main__':
    rate_buying, rate_selling, time = get_rate("https://minfin.com.ua/currency/auction/usd/buy/kiev/?order=newest")
    print(rate_buying, rate_selling)
