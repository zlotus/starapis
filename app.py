from apistar import Include, Route
from apistar.frameworks.wsgi import WSGIApp as App
from apistar.handlers import docs_urls, static_urls
from efunds import EFundsInfo
from xauusd import get_xauusd_by_duration
import json


def welcome(name=None):
    if name is None:
        return {'message': 'Welcome to API Star!'}
    return {'message': 'Welcome to API Star, %s!' % name}


def get_efunds_plan_list():
    with EFundsInfo() as efi:
        df = efi.e_funds_plan()
        return {'funds_summary': df.to_dict('records')}


def get_real_time_valuation(fund_code: str):
    with EFundsInfo() as efi:
        df = efi.real_time_valuation(fund_code)
        return {'fund_real_time_valuation': df.to_dict('records')}


def get_transaction_history(fund_code: str):
    with EFundsInfo() as efi:
        df = efi.transaction_history(fund_code)
        return {'transaction_history': df.to_dict('records')}


def get_value_history(fund_code: str, duration: str):
    with EFundsInfo() as efi:
        df = efi.fund_value_history(fund_code, duration).loc[:, ['date', 'value']]
        return {'value_history': df.to_dict('records')}


def get_xauusd_history(duration, from_date):
    return {'xauusd_history': get_xauusd_by_duration(duration, from_date)}

routes = [
    Route('/', 'GET', welcome),
    Route('/efunds/', 'GET', get_efunds_plan_list),
    Route('/efunds/<fund_code>/valuations/', 'GET', get_real_time_valuation),
    Route('/efunds/<fund_code>/transactions/', 'GET', get_transaction_history),
    Route('/efunds/<fund_code>/values/<duration>', 'GET', get_value_history),
    Route('/zlmxauusd/<duration>/from/<from_date>', 'GET', get_xauusd_history),
    Include('/docs', docs_urls),
    Include('/static', static_urls)
]

app = App(routes=routes)


if __name__ == '__main__':
    app.main()
