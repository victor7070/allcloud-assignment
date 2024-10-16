from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, month, year, current_timestamp, concat, sha2
from urllib.request import urlretrieve
from datetime import datetime
import zipfile
import pandas as pd
import logging as log
import random
import os

log.basicConfig(level=log.INFO)

INCREMENTAL_MODE = 'incremental'
REFRESH_MODE = 'refresh'


def get_spark():
    return SparkSession.builder.appName("my_app").getOrCreate()


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def get_latest_csv_path(folder_path='data/base/'):
    # get all csv files from folder
    files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    if not files:
        log.info(f"Couldn't find any file in {folder_path}")
        return
        
    # return the full path of the last item from the sorted list
    latest_csv_path = os.path.join(folder_path, sorted(files)[-1])
    log.info(f'The latest csv is {latest_csv_path}')
    
    return latest_csv_path


def extract_csv_from_repository(skip_download=True) -> str:
    if not skip_download:
        url = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'
        file_path = 'data/raw/online_retail.zip'
        
        log.info('Downloading the zip file')
        urlretrieve(url, file_path)
    
        log.info('Unzipping the file to get the excel')
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall('data/raw/')
    else:
        log.info('Skipping downloading and unzipping as they are time-consuming')

    log.info('Converting excel to csv with pandas')
    csv_path = f'data/base/online_retail_{get_datetime()}.csv'
    df = pd.read_excel('data/raw/Online Retail.xlsx')
    df.to_csv(csv_path, index=False)

    log.info(f'Successfully converted file {csv_path=}')
    return csv_path


def create_incremental_dummy_csv(latest_csv_path) -> str:
    log.info('Generating a dummy csv')
    df = pd.read_csv(latest_csv_path)

    # modify invoice date of first record (simulates record updated)
    df.at[0, 'InvoiceDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # add new record at the beginning of the file (simulates new record)
    item = {
        "InvoiceNo": "123456",
        "StockCode": str(random.randint(10000, 99999)),
        "Description": "description",
        "Quantity": 1,
        "InvoiceDate": '2021-01-01 10:00:00',
        "UnitPrice": 2.00,
        "CustomerID": 77777.0,
        "Country": "Romania",
    }

    new_row_df = pd.DataFrame([item], columns=df.columns)
    df = pd.concat([new_row_df, df], ignore_index=True)

    csv_path = f'data/base/online_retail_{get_datetime()}.csv'
    df.to_csv(csv_path, index=False)
    log.info(f'Generated a dummy csv {csv_path=}')
    
    return csv_path


def clean_data(df):
    # rename the columns using snake case convention
    new_column_names = {
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    }
    for old_col, new_col in new_column_names.items():
        df = df.withColumnRenamed(old_col, new_col)

    # data cleaning
    df = df.filter(df['customer_id'].isNotNull())  # drop records where customer_id is null
    df = df.withColumn("customer_id", col("customer_id").cast("int"))  # cast customer_id to int
    df = df.filter(~col('invoice_no').startswith('C'))  # remove cancelled invoices
    df = df.dropDuplicates(['invoice_no', 'stock_code', 'quantity', 'unit_price'])  # remove duplicate records
    
    return df


def transform_data(df):    
    # data transformation 2.1 a) and b)
    df = df.withColumn('total_amount', round(col('quantity') * col('unit_price'), 2))
    df = df.withColumn('order_year', year('invoice_date'))
    df = df.withColumn('order_month', month('invoice_date'))
    df = df.withColumn('last_processed_date', current_timestamp())

    # create a column by hashing multiple concatenated columns
    # this hash_id col should be unique across the entire dataset
    df = df.withColumn(
        "hash_id",
        sha2(concat(col("invoice_no"), col("stock_code"), col('quantity'), col("unit_price")), 256)
    )
    
    return df


def merge_dfs(new_df):
    spark = get_spark()
    existing_df = spark.read.csv('data/transformed/*.csv', header=True, inferSchema=True)
    
    merged_df = existing_df.union(new_df)
    merged_df.createOrReplaceTempView('df')
    merged_df = spark.sql("""
    with cta as (
        select
            hash_id,
            MAX(last_processed_date) as last_processed_date
        from df
        group by hash_id
    )
    select df.*
    from cta
    join df on df.hash_id = cta.hash_id and cta.last_processed_date = df.last_processed_date
    """)
    
    return merged_df


def extract(etl_mode):
    log.info(f'Started the EXTRACT phase with {etl_mode=}')

    spark = get_spark()

    if etl_mode == INCREMENTAL_MODE and (latest_csv_path := get_latest_csv_path()):
        dummy_csv_path = create_incremental_dummy_csv(latest_csv_path)
        dummy_df = spark.read.csv(dummy_csv_path, header=True, inferSchema=True)
        latest_df = spark.read.csv(latest_csv_path, header=True, inferSchema=True)
        log.info('Extracting the new/updated records between new and latest file')
        df = dummy_df.exceptAll(latest_df)
    else:
        repo_csv_path = extract_csv_from_repository()
        df = spark.read.csv(repo_csv_path, header=True, inferSchema=True)

    log.info(f'Finished the EXTRACT phase; Got a df with {df.count()} records \n')
    return df


def transform(df):
    log.info('Started the TRANSFORM phase')
    
    df = clean_data(df)
    df = transform_data(df)
    
    log.info(f'Finished the TRANSFORM phase; Got a df with {df.count()} records \n')
    return df


def load(df, etl_mode):
    log.info('Started the LOAD phase')

    if etl_mode == INCREMENTAL_MODE and get_latest_csv_path('data/transformed/'):
        log.info('Merging output files')
        df = merge_dfs(df)

    record_count = df.count()
    
    df.coalesce(1).write.csv("data/transformed/", header=True, mode="overwrite")
    
    log.info(f'Finished the LOAD phase; Got a df with {record_count} records \n')

def run_etl(etl_mode=INCREMENTAL_MODE):
    df = extract(etl_mode)
    df = transform(df)
    load(df, etl_mode)

    log.info('Finished ETL job')
