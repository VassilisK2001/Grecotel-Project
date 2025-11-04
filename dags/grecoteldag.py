from config.paths import ASSETS_DIR
from include.python.data_preprocessing import preprocess_data
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import  task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

# Fetch data from airflow connections
apify_conn = BaseHook.get_connection("apify_conn")
apify_token = apify_conn.password

deepl_conn = BaseHook.get_connection("deepl_conn")
deepl_auth_key = deepl_conn.password

json_path = ASSETS_DIR / 'dataset_tripadvisor-reviews_2025-11-01_14-21-09-431.json'

# Define database credentials
POSTGRES_CONN_ID = "postgres_conn"
_SCHEMA = "grecotel_db"
_HOTEL_TABLE = "hotel_table"
_REVIEW_TABLE = "review_table"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id = "tripadvisor_etl_pipeline",
    default_args = default_args,
    schedule = "@monthly",
    catchup = False,
    template_searchpath = ["/usr/local/airflow/include/sql"] 
) as dag:
    
    '''
    @task
    def extract(): 
        apify_client = ApifyClient(
            token = apify_token,
            max_retries = 6,
            min_delay_between_retries_millis = 500
        )
        actor_client = apify_client.actor('maxcopell/tripadvisor-reviews')

        with open(json_path, 'r', encoding='utf-8') as f:
            hotels = json.load(f) 
        
        # Define input for actor to run
        run_input = {
            "startUrls": [{"url": hotel["url"]} for hotel in hotels],
            "maxItemsPerQuery": 1000000,
            "scrapeReviewerInfo": True
        }

        try:

            # Start the actor
            call_result = actor_client.call(run_input=run_input)

            # Fetch results from the Actor run's default dataset 
            dataset_client = apify_client.dataset(call_result['defaultDatasetId'])
            list_items_result = dataset_client.list_items()

            return list_items_result.items

        except Exception as ApifyApiError:

            print(f"Apify API request failed: {ApifyApiError}")
        
        '''

    
    @task 
    def transform():
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
      
        extracted_data = []

        # List of keys to check for in the 'item' dictionary.
        desired_keys = ['id', 'lang', 'rating', 'travelDate' ,'publishedDate', 'title', 'text', 'tripType', 'placeInfo']
        
        for item in data:

            # Keep only relevant info from each item
            item = {key: item.get(key) for key in desired_keys}

            item['user_location'] = ((item.get('user') or {}).get('userLocation') or {}).get('name')

            # Handle nested keys with a specific check
            place_info = item.get('placeInfo', {}) 
            item['hotel_id'] = place_info.get('id')
            item['hotel_name'] = place_info.get('name')
            item['hotel_rating'] = place_info.get('rating')
            item['hotel_n_reviews'] = place_info.get('numberOfReviews')
            item['hotel_location'] = place_info.get('locationString') 

            extracted_data.append(item)

        df = pd.DataFrame(extracted_data)

        # Preprocess dataframe here
        transformed_df = preprocess_data(df, deepl_auth_key)

        return transformed_df

    @task
    def load(transformed_df: pd.DataFrame):
        conn = None
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()

            hotel_tuples = transformed_df[['hotel_id', 'hotel_name', 'hotel_rating', 'hotel_n_reviews',
                                           'hotel_location']].values.tolist()
              
            review_tuples = transformed_df[['id', 'lang', 'rating', 'review_text',
                                            'travelDate', 'publishedDate', 'tripType', 'user_location',
                                            'hotel_id']].values.tolist()

            hotels_insert = f"""
            INSERT INTO {_SCHEMA}.{_HOTEL_TABLE} (hotel_id, hotel_name, hotel_rating, n_reviews, hotel_location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (hotel_id) DO NOTHING;
            """
            reviews_insert = f"""
            INSERT INTO {_SCHEMA}.{_REVIEW_TABLE} (
                review_id, review_lang_original, review_rating, review_text_en, travel_date, review_published_date, trip_type, reviewer_location, hotel_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING;
            """

            with conn.cursor() as cur:
                cur.executemany(hotels_insert, hotel_tuples)
                cur.executemany(reviews_insert, review_tuples)
                conn.commit()
        
        except Exception as e:
            print(f"Error occured while loading data into database: {e}")

            if conn:
                conn.rollback()

        finally:

            if conn:
                conn.close()

    
    # DAG Workflow-ETL Pipeline
    create_tables = SQLExecuteQueryOperator(
        task_id= "create_tables",
        conn_id= POSTGRES_CONN_ID,
        sql="create_tables.sql",
        params={
            "schema": _SCHEMA,
            "hotel_table": _HOTEL_TABLE,
            "review_table": _REVIEW_TABLE
        } 
    )
    # extract_data = extract() 
    transformed_data = transform() 
    load_data = load(transformed_data)

    # Define task order
    create_tables >> transformed_data  >> load_data