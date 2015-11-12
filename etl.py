#!/usr/bin/env python3

import getpass
# import datetime
# import sys
# import time
import psycopg2  # postgresql

import pygrametl
from pygrametl.datasources import CSVSource  # , MergeJoiningSource
from pygrametl.tables import Dimension, FactTable


# CONFIGURATION

db_dw = 'fklubdw'
db_rw = 'fkluboltp'
csv_dir = 'fklubdw/FKlubSourceData/'
tables_names = []

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

for t in conn_dw.fetchalltuples():
    print(t)

# DIMENSIONS

dim_room = Dimension(
    name='room',
    key='room_id',
    attributes=['name', 'description']
)

for row in src_room:
    print(row)
    dim_room.insert(row)

"""
dim_member = Dimension(
    name='member',
    key='member_id',
    attributes=['is_active', 'year', 'balance']
)
"""

# FACT TABLE

fact_sale = FactTable(
    name='sale',
    keyrefs=['room_id'],  # ,'time_id', 'member_id', 'product_id'
    measures=['price'],
    # lookupatts=['price']
)

# PROGRAM

# [dim_room.insert(row) for row in src_room]


def main():
    print("main")


if __name__ == '__main__':
    main()

conn_dw.commit()
conn_dw.close()
pgconn_dw.close()
print("Done")
