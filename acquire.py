import pandas as pd
import os
from env import get_db_url

def acquire(use_cache = True):
    
    filename = 'curriculum_logs.csv'
    
    if os.path.exists(filename) and use_cache:
        print('Using cached .csv file...')
        return pd.read_csv(filename)
    
    else:
        print('Acquiring data from SQL server...')
        query = """SELECT * FROM logs LEFT JOIN cohorts ON logs.user_id = cohorts.id"""
        df = pd.read_sql(query, get_db_url('curriculum_logs'))
        
        df.to_csv(filename)
        
        return df
