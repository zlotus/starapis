from requestium import Session, Keys
import pandas as pd
import re
import json
import tushare as ts
import arrow


class EFundsInfo:
    def __init__(self):
        self.session = Session(webdriver_path='/usr/lib/chromium-browser/chromedriver',
                               browser='chrome',
                               default_timeout=15,
                               webdriver_options={'arguments': ['headless']})

    def __enter__(self):
        self.session = Session(webdriver_path='/usr/lib/chromium-browser/chromedriver',
                               browser='chrome',
                               default_timeout=15,
                               webdriver_options={'arguments': ['headless']})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.driver.quit()

    def e_funds_plan(self):
        self.session.driver.get("https://qieman.com/longwin/index")
        plan_div = self.session.driver.ensure_element_by_xpath("//section[@class='plan-asset']")
        plan_list = []
        for i, tr in enumerate(plan_div.find_elements_by_xpath("div//table[2]//tr")[1:], start=1):
            summary_list = tr.text.splitlines()
            abbreviation = summary_list[0]
            fund_name = summary_list[1][:-8]
            fund_code = summary_list[1][-7:-1]
            own_amount = re.compile("[持有](\d+)[份]").search(summary_list[2]).group(1)
            proportion = re.compile("[：]([-\d\.]+)").search(summary_list[2]).group(1)
            floating_pl = re.compile("[：]([-\d\.]+)").search(summary_list[3]).group(1)
            plan_list.append({
                'key': i,
                'abbreviation': abbreviation,
                'fund_name': fund_name,
                'fund_code': fund_code,
                'own_amount': own_amount,
                'proportion': proportion,
                'floating_pl': floating_pl,
            })
        df = pd.DataFrame(plan_list)
        df.key = pd.to_numeric(df.key)
        df.own_amount = pd.to_numeric(df.own_amount)
        df.proportion = pd.to_numeric(df.proportion)
        df.floating_pl = pd.to_numeric(df.floating_pl)
        return df

    def transaction_history(self, func_code):
        history = []
        today = arrow.now().format('YYYY-MM-DD')
        self.session.driver.get("https://qieman.com/longwin/funds/{func_code}".format(func_code=func_code))
        history_div = self.session.driver.ensure_element_by_xpath("//section[@class='history']")
        detail_div = self.session.driver.ensure_element_by_xpath("//section[@class='details']")
        amount_div_list = detail_div.find_elements_by_xpath("div//span[@class='qm-amount']")
        average_price, latest_price = amount_div_list[0].text, amount_div_list[1].text
        history.append({'key': 'a', 'date': today, 'price': average_price, 'action': 'a'})
        history.append({'key': 'y', 'date': today, 'price': latest_price, 'action': 'y'})
        for idx, td in enumerate(history_div.find_elements_by_xpath("table/tbody/tr")):
            deal_date = td.find_element_by_xpath("td//div[@class='variety-title']").text
            deal_price = td.find_element_by_xpath("td//span[@class='qm-amount']").text
            action_text = td.find_element_by_xpath("td//div[@class='order-action']").text
            action = "b" if "买" in action_text else "s"
            amount = pd.to_numeric(re.compile("[入|出](\d+)[份]").search(action_text).group(1))
            history.extend([{
                'key': '{index}{action}{count}'.format(index=idx, action=action, count=i),
                "date": deal_date, "price": deal_price,
                "action": action
            } for i in range(amount)])
        # df = pd.DataFrame(history).set_index("date")
        df = pd.DataFrame(history)
        # df.index = pd.to_datetime(df.index)
        df.price = pd.to_numeric(df.price)
        return df

    def e_fund_cost(self, func_code):
        self.session.driver.get("https://qieman.com/longwin/funds/{func_code}".format(func_code=func_code))
        detail_div = self.session.driver.ensure_element_by_xpath("//section[@class='details']")
        cost = detail_div.find_element_by_xpath("div//span[@class='qm-amount']").text
        return pd.to_numeric(cost)

    def fund_value_history(self, fund_code, duration='1m'):
        """
        Query fund trading history data from Sina finance
        :param duration: string
            default is '1m', means query one month history.
            OR using one of following:
                '1m' - one month history,
                '3m' - three month history,
                '6m' - six month history,
                '1y' - one year history,
                '2y' - two year history,
                '3y' - three year history.
        :param fund_code: string
            specify the code of the fund you want to query
        :return:
            DataFrame:
                date - index, trading date,
                value - fund net / annual income
                total - accumulated net value / fund million return
                change - fund net growth rate
        """
        result = []
        kv = {'1m': -1, '3m': -3, '6m': -6, '1y': -12, '2y': -24, '3y': -36}
        duration_arrow = self.get_last_trading_info(fund_code)['date'].shift(months=kv.get(duration, -1))
        df = ts.get_nav_history(fund_code, duration_arrow).reset_index()
        df.date = df.date.astype(str)
        return df

    def get_last_trading_date(self, fund_code):
        today = arrow.now().shift(months=-1)
        while True:
            latest_df = ts.get_nav_history(fund_code, today.format('YYYY-MM-DD'))
            if (latest_df is not None):
                return arrow.get(latest_df.index[0])
            else:
                today = today.shift(months=-1)

    def get_last_trading_info(self, fund_code):
        today = arrow.now().shift(months=-1)
        while True:
            latest_df = ts.get_nav_history(fund_code, today.format('YYYY-MM-DD'))
            if (latest_df is not None):
                return {'date': arrow.get(latest_df.index[0]), 'price': latest_df.value[0]}
            else:
                today = today.shift(months=-1)

    def real_time_valuation(self, fund_code: str):
        if fund_code == '001061':
            latest_info = self.get_last_trading_info(fund_code)
            # there is no real time valuation api for 001061
            valuation_date = latest_info['date']
            real_time_value_list = [['0930', latest_info['price']], ['1500', latest_info['price']]]
        elif fund_code.startswith('16'):
            # res = self.session.get("http://qt.gtimg.cn/q=sz{func_code}".format(func_code=func_code))
            res = self.session.get(
                "http://data.gtimg.cn/flashdata/hushen/minute/sz{func_code}.js".format(func_code=fund_code))
            real_time_value_list = []
            data_list = res.text.replace('\\n\\', '').splitlines()
            valuation_date = '{year}-{month}-{day}'.format(year='20' + data_list[1][-6:-4],
                                                           month=data_list[1][-4:-2],
                                                           day=data_list[1][-2:])
            for i in data_list[2:-1]:
                time, value, _ = i.split()
                real_time_value_list.append([time, value])
        else:
            res = self.session.get(
                "http://web.ifzq.gtimg.cn/fund/newfund/fundSsgz/getSsgz?app=web&symbol=jj{func_code}".format(
                    func_code=fund_code))
            json_dict = json.loads(res.text)['data']
            valuation_date = json_dict['date']
            real_time_value_list = json_dict['data']
        result = []
        for i in real_time_value_list:
            result.append({
                # 'time': '{date} {hour}:{miniute}:00'.format(date=valuation_date, hour=i[0][:2], miniute=i[0][2:]),
                'time': '{hour}:{miniute}'.format(hour=i[0][:2], miniute=i[0][2:]),
                'value': i[1],
            })

        df = pd.DataFrame(result)
        df.value = pd.to_numeric(df.value)
        return df  # f100032 = EFundsInfo()

# print(f100032.e_funds_plan().loc[1, 'func_code'])
# print(f100032.e_fund_history('100032'))
# print(f100032.e_fund_cost())
# print(f100032.market_fund_history(duration='3m'))
# print(f100032.real_time_valuation())
