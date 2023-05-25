from dvd_rental.pipeline.extract_load_pipeline import ExtractLoad
from graphlib import TopologicalSorter
import os
from database.postgres import PostgresDB
from dvd_rental.etl.transform import Transform

def run_pipeline():
    
    #configure pipeline
    path_extract_model = "dvd_rental/models/extract"
    path_transform_model = "dvd_rental/models/transform"
    path_extract_log = "dvd_rental/log/extract_log"
    source_engine = PostgresDB.create_pg_engine(db_target="source")
    target_engine = PostgresDB.create_pg_engine(db_target="target")
    
    # build dag
    dag = TopologicalSorter()
    nodes_extract_load = []
    
    # extract_load nodes
    for file in os.listdir(path_extract_model):
        node_extract_load = ExtractLoad(source_engine=source_engine, target_engine=target_engine,table_name=file.replace(".sql", ""), path=path_extract_model, path_extract_log=path_extract_log)
        dag.add(node_extract_load)
        nodes_extract_load.append(node_extract_load)
        
    # transform nodes
    node_staging_films = Transform("staging_films", engine=target_engine, models_path=path_transform_model)
    node_serving_sales_film = Transform("serving_sales_film", engine=target_engine, models_path=path_transform_model)
    node_serving_films_popular = Transform("serving_films_popular", engine=target_engine, models_path=path_transform_model)
    node_serving_sales_customer = Transform("serving_sales_customer", engine=target_engine, models_path=path_transform_model)
    node_serving_sales_cumulative = Transform("serving_sales_cumulative", engine=target_engine, models_path=path_transform_model)
    dag.add(node_staging_films, *nodes_extract_load)
    dag.add(node_serving_sales_film, node_staging_films, *nodes_extract_load)
    dag.add(node_serving_films_popular, node_staging_films, *nodes_extract_load)
    dag.add(node_serving_sales_customer, *nodes_extract_load)
    dag.add(node_serving_sales_cumulative, *nodes_extract_load)

    # run dag 
    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered: 
        node.run()


if __name__ == "__main__":
    run_pipeline()