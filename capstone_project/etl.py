from os import path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


output_folder = './datalake'

def get_spark_session():
    """
        Returns a spark session.
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()    
    
    return spark

def process_lookup_labels(spark):
    """
        Creates country, port and mode dimensions from labels file.
        
        Args:
            spark {object}: A spark session.
    """
    with open('I94_SAS_Labels_Descriptions.SAS') as f:
        lines = f.readlines()
        
    print("Countries...")
    dict_values = {}
    for line in lines[9:298]:
        code, name = line.split('=')
        code, name = code.strip(), name.strip().strip("'")
        dict_values[code] = name
    
    spark.createDataFrame(dict_values.items(), ['cod_country', 'country'])\
         .write.mode("overwrite")\
         .parquet(path=path.join(output_folder, 'country'))
    
    print("Ports...")
    dict_values = {}
    for line in lines[302:660]:
        code, name = line.split('=')
        code, name = code.strip(), name.strip().strip("'")
        dict_values[code] = name
    
    spark.createDataFrame(dict_values.items(), ['cod_port', 'port'])\
         .write.mode("overwrite")\
         .parquet(path=path.join(output_folder, 'port'))

    print("Modes...")
    dict_values = {}
    for line in lines[972:976]:
        code, name = line.split('=')
        code, name = code.strip(), name.strip().replace(";", "").replace("'", "")
        print(code, name)
        dict_values[code] = name
    
    spark.createDataFrame(dict_values.items(), ['cod_mode', 'mode'])\
         .write.mode("overwrite")\
         .parquet(path=path.join(output_folder, 'mode'))
    
    print("Visa...")
    dict_values = {}
    for line in lines[1046:1049]:
        code, name = line.split('=')
        code, name = code.strip(), name.strip()
        dict_values[code] = name
    
    spark.createDataFrame(dict_values.items(), ['cod_visa', 'visa'])\
         .write.mode("overwrite")\
         .parquet(path=path.join(output_folder, 'visa'))
    
    
def process_demographic_dim(spark):
    """
        Creates demographic dimension from csv file.
        
        Args:
            spark {object}: A spark session.
    """
    df = spark.read.option('delimiter', ';') \
                .option('header', True) \
                .csv("us-cities-demographics.csv")
    
    df.createOrReplaceTempView("demo")
    
    df = spark.sql("""
        SELECT `State Code` as state_code,
                `State` as state, 
                SUM(`Male Population`) as male_population, 
                SUM(`Female Population`) as female_population, 
                SUM(`Total Population`) as total_population, 
                SUM(`Number of Veterans`) as number_veterans, 
                SUM(`Foreign-born`) as foreign_born 
        FROM demo
        GROUP BY state_code, state
    """)
    
    df.write.mode("overwrite").parquet(path=path.join(output_folder, "us_demographic"))
    
def process_immigration_fact(spark):
    """
        Creates immigration fact from SAS files.
        
        Args:
            spark {object}: A spark session.
    """
    df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    df.createOrReplaceTempView("immigration")
    
    df = spark.sql("""
    SELECT cast(cicid as int) as id_cic, 
           cast(i94yr as int) as year, 
           cast(i94mon as int) as month, 
           cast(i94cit as int) as cod_country_cit, 
           cast(i94res as int) as cod_country_res, 
           i94port as cod_port,
           date_add('1960-1-1', arrdate) as arrival_date, 
           cast(i94mode as int) as cod_mode, 
           i94addr as code_state, 
           date_add('1960-1-1', depdate) as departure_date, 
           cast(i94bir as int) as age, 
           cast(i94visa as int) as cod_visa,
           cast(count as int) as count, 
           cast(biryear as int) as birth_year, 
           to_date(dtaddto, 'MMddyyyy') as date_admitted, 
           gender,
           airline, 
           admnum as adm_num, 
           fltno as fligh_number, 
           visatype as visa_type
    FROM immigration
    """)
    
    df.write.mode("overwrite").partitionBy('year', 'month').parquet(path=path.join(output_folder, "immigration"))

if __name__ == '__main__':
    spark = get_spark_session() 
    print("Creating lookup dims...")
    process_lookup_labels(spark)
    print("Creating US demographic dim...")
    process_demographic_dim(spark)
    print("Creating immigration fact...")
    process_immigration_fact(spark)
    print("Done.")