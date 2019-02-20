# -*- coding: utf-8 -*-

import pandas as pd
import pyodbc
from datetime import datetime
pyodbc.autocommit = True

con = pyodbc.connect("DSN=malawihiveodbc", autocommit=True)

cursor = con.cursor()

cursor.execute('use etldb')

def cell_tower(y):
    query="select distinct cell_id from mno_cdr_active_7pm_to_7am where call_start_date like '2016-%'"
    df = pd.read_sql_query(query, con)
    return df

def cell_tower_uniqueid(y):
    query="select cell_id,avg(no_of_unique_id) from mno_daily_agg where call_start_date like '2016-%'"
    df = pd.read_sql_query(query, con)
    return df
    
    