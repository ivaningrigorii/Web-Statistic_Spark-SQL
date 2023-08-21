from pyspark.sql import SparkSession
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import requests
import json
from pyspark.sql import Window
import xml.etree.ElementTree as ET

# url для получения данных (в xml формате)
BASE_URL = "https://www.fedstat.ru/indicator/data.do?format=sdmx"

MONTHS = [
 'январь', 'февраль', 'март',
 'апрель', 'май', 'июнь',
 'июль', 'август', 'сентябрь',
 'октябрь', 'ноябрь', 'декабрь'
]

appName = 'Jupyter'
master = 'local'
spark = SparkSession \
.builder \
.appName(appName) \
.master(master) \
.config("spark.jars", "spark-xml_2.12-0.13.0.jar") \
.config("spark.sql.caseSensitive", "true") \
.getOrCreate()

df_dataset = None


# сбор данных в xml формате 
def extr_data():

    headers = {
        'Content-Type' : 'application/x-www-form-urlencoded'
    }

    with open("filters.json", "r") as file_with_filters_json:
        data = file_with_filters_json.read()
    
        filters_for_request = json.loads(data)
      
    response = requests.post(
        BASE_URL, 
        data=filters_for_request, 
        headers=headers
    )

    root = ET.fromstring(response.content.decode('utf-8'))
    tree = ET.ElementTree(root)
    tree.write("file.xml")


# Получение indicator_value
def get_indicator_values():
    result = list_indicator_values = df_dataset \
    .select(
        explode(
            col("ns0:DataSet.ns3:Series.ns3:Obs.ns3:ObsValue._value")
        ).alias("indicator_value"), monotonically_increasing_id().alias("counter")
    )
    
    w = Window.orderBy("counter")
    result = result.withColumn("increment_id", row_number().over(w)).drop("counter")
    
    return result


# Получение дат: date_to и date_from

def moths_to_number(df_text_months):
    df_dist_moths = spark.createDataFrame(MONTHS, StringType()) \
        .withColumn("month", monotonically_increasing_id()+1) 
    
    result = df_text_months.join(
        df_dist_moths,
        df_dist_moths.value == df_text_months._value,
        "left_outer"
    ) \
        .select("month", "increment_id") \
    
    return result


def get_date_values():
    df_years = df_dataset \
        .select(explode( col("ns0:DataSet.ns3:Series.ns3:Obs.ns3:Time")).alias("year") ) \
        .withColumn("counter", monotonically_increasing_id()) 

    df_text_months = df_dataset \
        .select(explode(col("ns0:DataSet.ns3:Series.ns3:Attributes.ns3:Value")).alias("vals")) \
        .withColumn("counter", monotonically_increasing_id()) \
        .select(col("vals").getItem(1).alias("vals"), "counter") \
        .select("vals._value", "counter")
    
    w = Window.orderBy("counter")
    df_years = df_years.withColumn("increment_id", row_number().over(w)).drop("counter")
    df_text_months = df_text_months.withColumn("increment_id", row_number().over(w)).drop("counter")
    
    
    df_months = moths_to_number(df_text_months)
    
    result = df_years.join(df_months, df_years.increment_id == df_months.increment_id,
                          "inner") \
                .select("year", "month", df_years.increment_id.alias("increment_id"))
    
    return result


def months_with_previos_date__how_moth():
    """ Получение даты начала и окончания периода. 
            По деф-ту к пред. месяцу """
 
    df_date_src = get_date_values()
    
    result = df_date_src \
        .withColumn("date_from", 
            date_format(
                concat(col('year'), lit('-'), col('month'), lit('-'), lit('01')), "yyyy-MM-dd"
            )
        ) \
        .withColumn("date_to", last_day(col("date_from"))) \
        .select("increment_id", "date_from", "date_to") \
    
    return result


# общие сведения о видах товаров, типах индексов, регионах
def create_frame_add_data():
    df_id_vals = df_dataset.select(
        explode(col("ns0:CodeLists.ns2:CodeList._id")).alias("_id_val"),
        monotonically_increasing_id().alias("counter")
    )
    
    df_vals = df_dataset.select(
        explode((col("ns0:CodeLists.ns2:CodeList.ns2:Code"))).alias("vals"),
        monotonically_increasing_id().alias("counter")
    )
    
    w = Window.orderBy("counter")
    df_id_vals = df_id_vals.withColumn("increment_id", row_number().over(w)).drop("counter")
    df_vals = df_vals.withColumn("increment_id", row_number().over(w)).drop("counter")

    result = df_id_vals.join(
            df_vals, 
            df_id_vals.increment_id == df_vals.increment_id, 
            "inner"
        ) \
        .select(col("_id_val"), col("vals")) \
    
    return result


# Получение сведений по общим для конкретных значений 
def get_text_values_for_id(id_values, concept):
    
    df_values = create_frame_add_data()
    
    filtered_df = df_values \
        .where(col("_id_val").like(concept)) \
        .select(col("vals").alias("val"))

    df_ids = filtered_df.select(
        explode(col("val._value")).alias("indicator_value"),
        monotonically_increasing_id().alias("counter")
    )
    
    df_texts = filtered_df.select(
        explode(col("val.ns2:Description._VALUE")).alias("text_value"),
        monotonically_increasing_id().alias("counter")
    )
    
    w = Window.orderBy("counter")
    df_ids = df_ids.withColumn("increment_id", row_number().over(w)).drop("counter")
    df_texts = df_texts.withColumn("increment_id", row_number().over(w)).drop("counter")
    
    result = df_ids.join(
            df_texts,
            df_ids.increment_id == df_texts.increment_id,
            "inner"
        ) \
        .where(col("indicator_value").isin(id_values)) \
        .select("indicator_value", "text_value") \
    
    return result


# получение region, type_indicator, goods если нужны будут
def get_region_and_type_indicator__series(pos_xml):
    
    res = list_regions_ids = df_dataset \
        .select(explode(col("ns0:DataSet.ns3:Series.ns3:SeriesKey.ns3:Value")).alias("vals")) \
        .select(col("vals").getItem(pos_xml).alias("elem"))

    concept = str(res.select("elem._concept").distinct().collect()[0][0])
    values = list(res.select(col("elem._value")).distinct().toPandas()['_value'])
    

    full_distinct_text_df = get_text_values_for_id(values, concept)


    df_column_of_values = res.select(col("elem._value"), 
                                         monotonically_increasing_id().alias("counter"))
    w = Window.orderBy("counter")
    df_column_of_values = df_column_of_values.withColumn("increment_id", row_number().over(w)).drop("counter")

    
    result = df_column_of_values.join(
            full_distinct_text_df, 
            full_distinct_text_df.indicator_value == df_column_of_values._value, 
            "inner"
        ) \
        .select("text_value", df_column_of_values.increment_id.alias("increment_id")) 
    
    return result


def get_xml_after_download():
    df_dataset = spark \
        .read.format("xml") \
        .option("RowTag", "ns0:GenericData") \
        .load("file.xml")
    return df_dataset


def save_result_df(df_regions, df_types_indicators,
                        df_indicator_values, df_dates):
    """ создание общего df и сохранение"""

    df_regions \
        .join(
            df_indicator_values,
            df_indicator_values.increment_id == df_regions.increment_id,
            "inner"
        ) \
        .join(
            df_dates,
            df_dates.increment_id == df_regions.increment_id,
            "inner"
        ) \
        .join(
            df_types_indicators,
            df_types_indicators.increment_id == df_regions.increment_id,
            "inner"
        ) \
        .select(
            df_regions.text_value.alias("region"),
            df_types_indicators.text_value.alias("type_indicator"),
            df_indicator_values.indicator_value.alias("indicator_value"),
            "date_from",
            "date_to"
        ) \
            .write.mode("overwrite") \
            .parquet("result/data_indicator.parquet")
    
    print("данные сохранены")



#в xml: 0 - регионы, 1 - тип индикатора
if __name__ == "__main__":
    extr_data()

    df_dataset = get_xml_after_download()

    df_regions = get_region_and_type_indicator__series(0)
    df_types_indicators = get_region_and_type_indicator__series(1)

    df_indicator_values = get_indicator_values()
    df_dates = months_with_previos_date__how_moth()

    et_df_result = save_result_df(df_regions, df_types_indicators,
                                    df_indicator_values, df_dates)

