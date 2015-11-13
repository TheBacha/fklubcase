#!/usr/bin/env python3

import getpass
import datetime
import calendar
import sys
import time
import psycopg2  # postgresql

import pygrametl
from pygrametl.datasources import CSVSource  # , MergeJoiningSource
from pygrametl.tables import Dimension, FactTable


# CONFIGURATION

db_dw = 'fklubdw'
db_rw = 'fkluboltp'
csv_dir = 'fklubdw/FKlubSourceData/'
tables_names = ['member', 'time', 'product', 'room', 'sale']

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
conn_rw.execute('set search_path to pygrametlexa') """

# pgconn_dw = psycopg2.connect("user='"+username+"' dbname='"+db_dw+"'")
pgconn_dw = psycopg2.connect(user=username, database=db_dw)
conn_dw = pygrametl.ConnectionWrapper(pgconn_dw)
conn_dw.setasdefault()
# conn_dw.execute('set search_path to pygrametlexa')

# TRUNCATE

[conn_dw.execute("TRUNCATE TABLE "+t+" CASCADE;") for t in tables_names]

# ROW EXPANDERS


# DIMENSIONS

dim_room = Dimension(
    name='room',
    key='room_id',
    attributes=['name', 'description']
)

dim_member = Dimension(
    name='member',
    key='member_id',
    attributes=['is_active', 'year', 'balance']
)

dim_product = Dimension(
    name='product',
    key='product_id',
    attributes=['name', 'price', 'is_active', 'deactivation_date']  # 'type'
)

dim_time = Dimension(
    name='time',
    key='time_id',
    attributes=['semester', 'week', 'day', 'hour', 'quarter_hour', 'year',
                'is_spring', 'is_weekend', 'is_morning', 'is_afternoon']
)

# FACT TABLE

fact_sale = FactTable(
    name='sale',
    keyrefs=['room_id', 'time_id', 'member_id', 'product_id'],
    measures=['price'],
    # lookupatts=['price']
)

# PROGRAM


def GetMinMaxDate(rounded=True):
    min_s = '1996-10-28 12:21:23'
    max_s = '2008-01-07 12:19:39'
    fmt = '%Y-%m-%d %H:%M:%S'
    min_t = time.strptime(min_s, fmt)
    max_t = time.strptime(max_s, fmt)
    if(rounded):
        # min_f = time.mktime(min_t)
        # max_f = time.mktime(max_t)
        min_f = calendar.timegm(min_t)
        max_f = calendar.timegm(max_t)
        qrt_f = 15*60.0  # 15 minutes
        min_f -= min_f % qrt_f
        max_f += qrt_f  # round up instead of down
        max_f -= max_f % qrt_f
        min_t = time.gmtime(min_f)
        max_t = time.gmtime(max_f)
    return min_t, max_t


def TimeToRow(dt):
    """
           c <
          /\/    o    o    o    o
         /_    /\/) /\/) /\/) /\/)
       _[]/___/__/_/__/_/__/_/__/______
    -~-~ `---/----/----/----/-------'  -~-~
    -~-~-~-~  -~-~  -~-~-~-~
    """


def TimeGenerator():
    raise "meh"

# LOAD DATA

[dim_room.insert(row) for row in src_room]


def main():
    print("main")
    min_d, max_d = GetMinMaxDate(False)
    print (min_d)
    print (max_d)
    min_r, max_r = GetMinMaxDate()
    print (min_r)
    print (max_r)


if __name__ == '__main__':
    main()

conn_dw.commit()
conn_dw.close()
pgconn_dw.close()
print("Done")
