import arrow
import sqlite3


def get_xauusd_by_duration(duration=None, from_date=None):
    '''
    query a period of time xauusd data from DB
    :param duration:
        default return last 4 hours data, other options are:
        '4h' - 4 hours
        '1d' - 1 day
        '1w' - 1 week
        '2w' - 2 weeks
        '1m' - 1 month
        '3m' - 3 months
        '6m' - 6 months
        '1y' - 1 year
        '2y' - 2 years
        '3y' - 3 years
    :return:
    '''
    if duration is None:
        duration = '4h'
    if from_date is None:
        from_date = arrow.now()
    arrow_date = arrow.get(from_date)
    kv = {
        '4h': {'hours': -4},
        '1d': {'days': -1},
        '1w': {'weeks': -1},
        '2w': {'weeks': -2},
        '1m': {'months': -1},
        '3m': {'months': -3},
        '6m': {'months': -6},
        '1y': {'years': -1},
        '2y': {'years': -2},
        '3y': {'years': -3},
    }
    with sqlite3.connect('/home/pi/pyapps/zlm/app.db') as conn:
        cur = conn.cursor()
        result = []
        for row in cur.execute('SELECT * FROM xauusd_sequencial WHERE id > ? AND id < ?',
                               (arrow_date.shift(**kv[duration]).timestamp, arrow_date.timestamp)):
            row = list(row)
            row[0] = arrow.get(row[0]).format('YYYY-MM-DD HH:mm:ss')
            result.append(row)
        return result
