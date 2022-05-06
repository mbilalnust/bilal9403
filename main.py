#Import Package
import psycopg2
from ast import literal_eval
import time
import pandas as pd            
import json
from fastapi import FastAPI

# ## Data Extraction

#connection information
con_ts = "dbname =tiot user=postgres password=DpW0rld!@ host=172.21.4.200 port=6432"
con_pg = "dbname =tiot user=msa_user password=DpW0rld!@ host=172.21.4.200 port=5432"
query= ''' select utct ,eqnm,
            contents ->'Body' ->> 'SPED' speed,
            contents ->'Body' ->> 'LATI' LATI,
            contents ->'Body' ->> 'LONG' LONG,
            contents ->'Body' ->> 'DIRE' direction
            from msg_periodic_rtls mpr 
            where mpr.teid = 'BCT'
            and eqty = 'YT'
            and mpr.eqnm = '257'
            order by utct desc
            limit 200'''

def ext_msg(db_info, query):    
    extracting_start = time.time()
    with psycopg2.connect(db_info) as conn:
        conn = psycopg2.connect(db_info)
        cur = conn.cursor()        
        df = pd.read_sql(query,con=conn)
        print("extracting execute time :", time.time() - extracting_start)
   
    return json.loads(df.to_json(orient='records'))
    
first_test = ext_msg(con_ts,query)
extracting_start = time.time()
with psycopg2.connect(con_ts) as conn:
    conn = psycopg2.connect(con_ts)
    cur = conn.cursor()        
    df = pd.read_sql(query,con=conn)
    print("extracting execute time :", time.time() - extracting_start)
    print(json.loads(df.to_json(orient='records')))
    x = json.loads(df.to_json(orient='records'))

app = FastAPI()
@app.get("/")
async def root():
    return x
