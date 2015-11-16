#!/usr/bin/env python3

import getpass
import datetime as dt
from subprocess import call, check_call
import psycopg2  # postgresql

import pygrametl
from pygrametl.datasources import CSVSource, MergeJoiningSource, HashJoiningSource
from pygrametl.tables import Dimension, FactTable


# CONFIGURATION

DEBUG = not False
KEEP_TIME = False
VERBOSE = False

db_dw = 'fklubdw'
db_rw = 'fkluboltp'
csv_dir = 'fklubdw/FKlubSourceData/'
tables_names = ['member', 'time', 'product', 'room', 'sale']
quarter = dt.timedelta(minutes=15)
min_s = '1996-10-28 12:21:23'
max_s = '2008-01-07 12:19:39'

username = getpass.getuser()
print ('user='+username)


# DATA SOURCES

def LoadDataSource(name):
    return CSVSource(open(csv_dir+name, 'r', 16384), delimiter='\t')

src_member = LoadDataSource('Member.txt')
src_payment = LoadDataSource('Payment.txt')
src_product = LoadDataSource('Product.txt')
src_room = LoadDataSource('Room.txt')
src_sale = LoadDataSource('Sale.txt')
src_sem_group = LoadDataSource('SemesterGroups.txt')


# DATABASE CONNECTION

"""
pgconn_rw = psycopg2.connect(user=username, database=db_rw)
conn_rw = pygrametl.ConnectionWrapper(pgconn_rw)
conn_rw.execute('set search_path to pygrametlexa')
"""

# pgconn_dw = psycopg2.connect("user='"+username+"' dbname='"+db_dw+"'")
pgconn_dw = psycopg2.connect(user=username, database=db_dw)
conn_dw = pygrametl.ConnectionWrapper(pgconn_dw)
conn_dw.setasdefault()
# conn_dw.execute('set search_path to pygrametlexa')


# TRUNCATE

# check_call(['psql', 'fklubdw', '-f', 'scripts/dw_create_tables.sql'])
for t in tables_names:
    if KEEP_TIME and t == 'time': continue
    conn_dw.execute("TRUNCATE TABLE "+t+" CASCADE;")


# ROW EXPANDERS


def TimeExpander(row, namemapping):
    if VERBOSE: print('TimeExpander:', namemapping, row)
    ts = pygrametl.getvalue(row, 'timestamp', namemapping)
    date = TimestampToDateTime(ts)
    timerow = TimeToRow(date)
    row.update(timerow)
    if VERBOSE: print('Expended row:', row)
    # row[namemapping['timestamp']] = str(date)
    return row

# DIMENSIONS

dim_room = Dimension(name='room', key='room_id',
    attributes=['name', 'description'])

dim_member = Dimension(name='member', key='member_id',
    attributes=['is_active', 'year', 'balance'])

dim_product = Dimension(name='product', key='product_id',
    attributes=['name', 'price', 'is_active', 'deactivation_date'])
    # FIXME 'type' is missing from attributes

# Approx. 28 MB.
dim_time = Dimension(name='time', key='time_id',
    attributes=['semester', 'week', 'day', 'hour', 'quarter_hour', 'year',
                'is_spring', 'is_weekend', 'is_morning', 'is_afternoon',
                # Ignored:
                'is_holiday', 'event_flan'],
    lookupatts=['time_id'],
    rowexpander=TimeExpander)


# FACT TABLE

fact_sale = FactTable(
    name='sale',
    keyrefs=['room_id', 'time_id', 'member_id', 'product_id'],
    measures=['price'],
    # lookupatts=['price']
)


# PROGRAM


def TimestampToDateTime(ts):
    fmt = '%Y-%m-%d %H:%M:%S' if ts[5] == '-' and ts[8] == '-' else '%d-%m-%Y %H:%M:%S'
    return dt.datetime.strptime(ts, fmt)


def GetMinMaxDate(rounded=True):
    min_dt = TimestampToDateTime(min_s)
    max_dt = TimestampToDateTime(max_s)
    if(rounded):
        min_dt -= (min_dt - dt.datetime.min) % quarter
        max_dt -= (max_dt - dt.datetime.min) % quarter - quarter  # round up
    return min_dt, max_dt


def GetSemesterWeek(t):
    weeknum = int(t.strftime('%W'))
    # FIXME Calculate week of semester.
    return weeknum


def TimeIdFromDateTime(t):
    q_h = int(t.minute / 15)
    return (366 * (t.year - 1990) + t.timetuple().tm_yday)*24*4 \
           + t.hour*4 + q_h, q_h


def TimeToRow(t):
    """
           c <
          /\/    o    o    o    o
         /_    /\/) /\/) /\/) /\/)
       _[]/___/__/_/__/_/__/_/__/______
    -~-~ `---/----/----/----/-------'  -~-~
        -~-~-~-~  -~-~  -~-~-~-~
    """
    if VERBOSE: print('TimeToRow:', t)
    # q_h = int(t.minute / 15)
    time_id, q_h = TimeIdFromDateTime(t)
    assert 0 <= q_h < 4
    is_spring = 1 < t.month < 8
    day = dt.datetime.combine(t.date(), dt.time.min)
    weekday = t.isoweekday()
    row = {
        'time_id': time_id,
        'semester': ('F' if is_spring else 'E') + t.strftime('%y'),
        'week': GetSemesterWeek(t),
        'day': weekday,
        'hour': t.hour,
        'quarter_hour': q_h,
        'year': t.year,
        'is_spring':  is_spring,
        'is_weekend': 5 < weekday,
        'is_morning': day.replace(hour=8) <= t <= day.replace(hour=12),
        'is_afternoon': day.replace(hour=12, minute=30) <= t
            <= day.replace(hour=16, minute=30),
        # Dummy values:
        'event_flan': False,
        'is_holiday': False,
    }
    if(VERBOSE): print(row)
    return row


"""
def SplitTimestamp(row, col='timestamp'):
    t = TimestampToDateTime(row[col])
    print(t)
    row.update(TimeToRow(t))
"""


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


"""
def TimeGenerator():
    date_min, date_max = GetMinMaxDate(True)
    num_rows = int((date_max - date_min) / quarter) + 1
    print('time rows:', num_rows)
    start = date_min
    delta = quarter if not DEBUG else quarter * 2077
    end = date_max if not DEBUG else start + delta * 10
    # TimeToRow(date_min)
    t_start = dt.datetime.utcnow()
    for tr in perdelta(start, end, delta):  # 1-2 min. to run.
        dim_time.insert(TimeToRow(tr))
    dim_time.insert(TimeToRow(date_max))
    t_stop = dt.datetime.utcnow()
    print('Elapsed:', t_stop - t_start)
"""


def IntToMoney(row, name):
    row[name] = float(row[name]) / 100


def main():
    print("MAIN")
    # LOAD DATA:
    # if not KEEP_TIME: TimeGenerator()

    lut_room = {}
    lut_member = {}
    lut_product = {}

    for r in src_room:
        rid = dim_room.insert(r)
        lut_room[r['id']] = str(rid)
    for m in src_member:
        IntToMoney(m, 'balance')
        mid = dim_member.insert(m, {'is_active': 'active'})
        lut_member[m['id']] = str(mid)
    for p in src_product:
        IntToMoney(p, 'price')
        if p['deactivatedate'] == '':
            p['deactivation_date'] = None
        else:
            tid = TimeIdFromDateTime(TimestampToDateTime(p['deactivatedate']))[0]
            p.update({'time_id': tid})
            p['deactivation_date'] = dim_time.ensure(p, {'timestamp': 'deactivatedate'})
        pid = dim_product.insert(p, {'deactivation_date': 'deactivatedate', 'is_active': 'active'})
        lut_product[p['id']] = str(pid)

    for s in src_sale:
        # if VERBOSE:
        IntToMoney(s, 'price')
        try:
            s['room_id'] = lut_room[s['room_id']]
            s['member_id'] = lut_member[s['member_id']]
            s['product_id'] = lut_product[s['product_id']]
        except KeyError as e:
            print('ERROR: Key not found:', e)
            print('Sale:', s)
            if DEBUG:
                input("Press Enter to continue...")
            print('Datum discarded, continuing...')
            continue
        tid = TimeIdFromDateTime(TimestampToDateTime(s['timestamp']))[0]
        s.update({'time_id': tid})
        s['time_id'] = dim_time.ensure(s)

        if VERBOSE: print('SALE TO INSERT:', s)
        fact_sale.insert(s)


if __name__ == '__main__':
    main()

conn_dw.commit()
conn_dw.close()
pgconn_dw.close()
print("DONE")
