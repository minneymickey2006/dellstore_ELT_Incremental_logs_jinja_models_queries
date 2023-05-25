from sqlalchemy import Integer, String, Float, JSON, DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON
import jinja2 as j2
import pandas as pd
import numpy as np
import logging
from sqlalchemy.dialects import postgresql

class Load():
    
    @staticmethod
    def get_key_columns(table:str, path:str="extract_queries")->list:
        """
        get a list of key columns from .sql file.
        - 'table': name of the sql file without .sql.
        - 'path': path to the sql file
        """
        # read the sql contents into a variable
        with open(f"{path}/{table}.sql") as f:
            raw_sql = f.read()
        try:
            key_columns = j2.Template(raw_sql).make_module().config["key_columns"]
            return key_columns
        except:
            return []
        
    @staticmethod
    def get_sqlalchemy_column(column_name:str, source_datatype:str, primary_key:bool=False)->Column:
        """
        A helper function that returns a SQLAlchemy column by mapping a pandas dataframe datatypes to sqlalchemy datatypes
        
        """
        dtype_map = {
            "int64":BigInteger,
            "object":String,
            "datetime64[ns]":DateTime,
            "float64": Numeric,
            "bool": Boolean
        }
        column = Column(column_name, dtype_map[source_datatype], primary_key=primary_key)
        return column
    
    @staticmethod
    def generate_sqlalchemy_schema(df: pd.DataFrame, key_columns:list, table_name, meta): 
        """
        Generates a sqlalchemy table schema that shall be used to create the target table and perform insert/upserts. 
        """
        schema = []
        for column in [{"column_name": col[0], "source_datatype": col[1]} for col in zip(df.columns, [dtype.name for dtype in df.dtypes])]:
            schema.append(Load.get_sqlalchemy_column(**column, primary_key=column["column_name"] in key_columns))
        return Table(table_name, meta, *schema)

    @staticmethod
    def upsert_in_chunks(df:pd.DataFrame, engine, table_schema:Table, key_columns:list, chunksize:int=1000)->bool:
        """
        performs the upsert with several rows at a time (i.e. a chunk of rows). this is better suited for very large sql statements that need to be broken into several steps. 
        """
        max_length = len(df)
        df = df.replace({np.nan: None})
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize
            insert_statement = postgresql.insert(table_schema).values(df.iloc[lower_bound:upper_bound].to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
            logging.info(f"Inserting chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
            result = engine.execute(upsert_statement)
        return True 
