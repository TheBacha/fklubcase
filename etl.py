#!/usr/bin/env python3

import getpass
import datetime as dt
# import sys
# import time
from subprocess import call, check_call
import psycopg2  # postgresql

import pygrametl
from pygrametl.datasources import CSVSource  # , MergeJoiningSource
from pygrametl.tables import Dimension, FactTable


# CONFIGURATION

DEBUG = False
KEEP_TIME = True
VERBOSE = False

db_dw = 'fklubdw'
db_rw = 'fkluboltp'
csv_dir = 'fklubdw/FKlubSourceData/'
tables_names = ['member', 'time', 'product', 'room', 'sale']
quarter = dt.timedelta(minutes=15)
min_s = '1996-10-28 12:21:23'
max_s = '2008-01-07 12:19:39'
fmt = '%Y-%m-%d %H:%M:%S'

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
    lookupatts=['semester', 'week', 'day', 'hour', 'quarter_hour', 'year',
                'is_spring', 'is_weekend', 'is_morning', 'is_afternoon',
                'is_holiday', 'event_flan'])


# FACT TABLE

fact_sale = FactTable(
    name='sale',
    keyrefs=['room_id', 'time_id', 'member_id', 'product_id'],
    measures=['price'],
    # lookupatts=['price']
)


# PROGRAM


def GetMinMaxDate(rounded=True):
    min_dt = dt.datetime.strptime(min_s, fmt)
    max_dt = dt.datetime.strptime(max_s, fmt)
    if(rounded):
        min_dt -= (min_dt - dt.datetime.min) % quarter
        max_dt -= (max_dt - dt.datetime.min) % quarter - quarter  # round up
    return min_dt, max_dt


def GetSemesterWeek(t):
    weeknum = int(t.strftime('%W'))
    # FIXME Calculate week of semester.
    return weeknum


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
    q_h = t.minute / 15
    assert 0 <= q_h < 4
    is_spring = 1 < t.month < 8
    day = dt.datetime.combine(t.date(), dt.time.min)
    weekday = t.isoweekday()
    row = {
        'semester': ('F' if is_spring else 'E') + t.strftime('%y'),
        'week': GetSemesterWeek(t),
        'day': weekday,
        'hour': t.hour,
        'quarter_hour': int(q_h),
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


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


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

# LOAD DATA

# [dim_room.insert(row) for row in src_room]


def main():
    print("MAIN")
    if not KEEP_TIME:
        TimeGenerator()


if __name__ == '__main__':
    main()

conn_dw.commit()
conn_dw.close()
pgconn_dw.close()
print("DONE")
