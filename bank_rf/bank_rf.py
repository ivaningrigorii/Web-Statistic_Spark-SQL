import os

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

import requests
from bs4 import BeautifulSoup
from datetime import datetime

import pyspark.pandas as pd


BASE_URL_DOMEN = "https://www.cbr.ru"
NOW_TIME_STR = datetime.now().strftime("%d.%m.%Y")

API_DATA = [
    {
        "increment":2,
        "indicator":"Задолженность (в том числе просроченная) по жилищным кредитам, предоставленным физическим лицам-резидентам",
        "url_path":"https://www.cbr.ru/statistics/bank_sector/mortgage/",
        "path_in_file": "итого",
        "physical_name":"housing_loan",
        "action_download": "download_excel_files",
        "action_transform": "tr_ex_files"
    },
    {
        "increment":22,
        "indicator":"Ключевая ставка Банка России",
        "url_path":f"https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True&UniDbQuery.From=17.09.2013&UniDbQuery.To={NOW_TIME_STR}",
        "physical_name":"cbr_rate",
        "action_download": "download_webtable",
        "action_transform": "tr_keyrate"
    },
    {
        "increment":23,
        "indicator":"Курс доллара США",
        "url_path":f"https://www.cbr.ru/currency_base/dynamics/?UniDbQuery.Posted=True&UniDbQuery.so=0&UniDbQuery.mode=1&UniDbQuery.date_req1=&UniDbQuery.date_req2=&UniDbQuery.VAL_NM_RQ=R01235&UniDbQuery.From=01.07.1992&UniDbQuery.To={NOW_TIME_STR}",
        "physical_name":"usd_rub",
        "action_download": "download_webtable",
        "action_transform": "tr_moneyrate"
    }
]


def download_excel_files(row_api_data):
    """ Загрузка EXCEL файлов из mortgage """
    response_html = requests.get(row_api_data["url_path"]).content.decode('utf-8')
    soup = BeautifulSoup(response_html)
        
    args = {"data-zoom-title": row_api_data['indicator']}
    for a in soup.find_all('a', args, href=True):
        url_for_download = a["href"]
        
    response_excel = requests.get(f"{BASE_URL_DOMEN}{url_for_download}")
        
    filepath = f"./src/{row_api_data['physical_name']}.xlsx"
        
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as output:
        output.write(response_excel.content)


def download_webtable(row_api_data):
    """ Парсинг табличек на сайте """
    url_path = row_api_data["url_path"]
    response_html = requests.get(url_path).content.decode('utf-8')
    
    df_pd = pd.read_html(response_html, thousands=None)[0]

    df_pd.to_spark() \
        .write \
        .option("header",True) \
        .mode('overwrite') \
        .csv(f"./src/{row_api_data['physical_name']}.xlsx")


functions_download = {
    "download_excel_files": download_excel_files,
    "download_webtable": download_webtable
}

def download_files():
    for api_row_data in API_DATA:
        functions_download[api_row_data["action_download"]](api_row_data)


def transform_excel_files(row_api_data):
    """ Преобразование загруженных excel файлов """
    path_in_file = row_api_data['path_in_file']
    
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("usePlainNumberFormat", "true") \
        .option("dataAddress", f"'{path_in_file}'!A2") \
        .load(f"./src/{row_api_data['physical_name']}.xlsx")
   
    df_regions = df.select(col("_c0").alias("region"))
    
    w = Window.orderBy("date_from")
    w_increment = Window.orderBy("counter")
    
    df_date_from = spark.createDataFrame(df.schema.names[1:], StringType()) \
        .select(to_date(col("value"), "dd.MM.yyyy").alias("date_from")) \
        .withColumn("date_to", lead(col("date_from")).over(w)) 
    
    df_get_vals = df.drop("_c0")
    vals = []
    for row in df_get_vals.collect():
        vals += list(row.asDict().values())
    
    df_vals = spark.createDataFrame(vals, FloatType()) \
        .withColumn("counter", monotonically_increasing_id()) \
        .withColumn("increment_id", row_number().over(w_increment)) \
        .drop(col("counter"))
    
    df_reg_dat = df_regions.crossJoin(df_date_from) \
        .withColumn("counter", monotonically_increasing_id()) \
        .withColumn("increment_id", row_number().over(w_increment)) \
        .drop(col("counter"))
    
    result = df_reg_dat \
        .join( df_vals, df_vals.increment_id == df_reg_dat.increment_id, "inner") \
        .drop(df_vals.increment_id) \
        .orderBy(col("increment_id")) \
        .withColumn("indicator", lit(row_api_data["indicator"])) \
        .where((~ col("region").like("%ФЕДЕРАЦИЯ")) & (~ col("region").like("%ОКРУГ"))) \
        .select("indicator", "region", "value", "date_from", "date_to")
    
    return result


def transform_keyrate_files(row_api_data):
    """ Преобразование файла ключевой ставки """  
    
    df = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header",True) \
        .load(f"./src/{row_api_data['physical_name']}.xlsx")
    
    w = Window.orderBy("date_from")
    
    result = df.withColumn("date_from", to_date(col("Дата"), "dd.MM.yyyy")) \
        .withColumn("value_tmp2", regexp_replace('Ставка', ' ', '')) \
        .withColumn("value_tmp", regexp_replace('value_tmp2', ',', '.')) \
        .withColumn("value", col('value_tmp').cast("float")) \
        .withColumn("region", lit(None)) \
        .withColumn("date_to", lead(col("date_from")).over(w)) \
        .withColumn("indicator", lit(row_api_data["indicator"])) \
        .select("indicator", "region", "value", "date_from", "date_to")
    
    return result


def transform_moneyrate_files(row_api_data):
    """ Преобрзование файла с курсом валюты (доллара) """
    schema = StructType() \
      .add("date_from_src",StringType(),True) \
      .add("skip(vals)",StringType(),True) \
      .add("value_src",StringType(),True)
    
    df = spark.read.format("csv") \
        .option("header", False) \
        .schema(schema) \
        .load(f"./src/{row_api_data['physical_name']}.xlsx")
    
    df = df \
        .withColumn("counter", monotonically_increasing_id()) \
        .where(col("counter") > 2)
    
    w = Window.orderBy("date_from")
    
    
    result = df.withColumn("date_from", to_date(col("date_from_src"), "dd.MM.yyyy")) \
        .withColumn("value_tmp2", regexp_replace('value_src', ' ', '')) \
        .withColumn("value_tmp", regexp_replace('value_tmp2', ',', '.')) \
        .withColumn("value", col('value_tmp').cast("float")) \
        .withColumn("region", lit(None)) \
        .withColumn("date_to", lead(col("date_from")).over(w)) \
        .withColumn("indicator", lit(row_api_data["indicator"])) \
        .select("indicator", "region", "value", "date_from", "date_to")
    
    return result


functions_transform = {
    "tr_ex_files": transform_excel_files,
    "tr_keyrate": transform_keyrate_files,
    "tr_moneyrate": transform_moneyrate_files
}

def transform():
    result = []
    for api_data in API_DATA:
        df = functions_transform[api_data["action_transform"]](api_data)
        result.append(df)
    return result


def save_result(lst_dfs):
    for i, df in enumerate(lst_dfs):
        if (i == 0):
            result = df
        result = result.union(df)
        
    list_columns = ["date_from", "indicator"]
    result = result.orderBy(*list_columns, ascending=False)
    
    #null date_to to current date
    result = result \
        .withColumn("date_to_withoutNull", when(~ col("date_to").isNull(), col("date_to")) \
            .when(col("date_to").isNull(), current_date())) \
        .drop("date_to") \
        .select("indicator", "region", "value", "date_from",
            col("date_to_withoutNull").alias("date_to"))

    result \
        .write.mode("overwrite") \
        .parquet("result/data")


def main():
    download_files()
    lst_dfs = transform()
    save_result(lst_dfs)

    
if __name__ == "__main__":

    appName = 'bank_rf'
    master = 'local'

    spark = SparkSession \
        .builder \
        .appName(appName) \
        .master(master) \
        .config("spark.jars", "spark-xml_2.12-0.13.0.jar") \
        .config("spark.jars", "com.crealytics_spark-excel_2.12-3.3.1_0.18.5") \
        .getOrCreate()

    main()
