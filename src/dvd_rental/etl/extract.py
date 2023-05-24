import pandas as pd
import jinja2 as j2
import logging
import os
import datetime as dt

class Extract():
    
    @staticmethod
    def get_incremental_value(table_name, path="extract_log"):
        df = pd.read_csv(f"{path}/{table_name}.csv")
        return df[df["log_date"] == df["log_date"].max()]["incremental_value"].values[0]
    
    
    @staticmethod
    def is_incremental(table:str, path:str)->bool:
        with open(f"{path}/{table}.sql") as f:
            raw_sql = f.read()
        try:
            config = j2.template(raw_sql).make_module().config
            return config["extract_type"].lower() == "incremental"
        except:
            return False
        
        @staticmethod    