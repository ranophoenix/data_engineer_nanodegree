from pyspark.sql import SparkSession
from os import path

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

def check_lookup_labels(spark, expected_totals):
    """
        Check totals from country, port and mode dimensions.
        
        Args:
            spark {object}: A spark session.
            expected_totals {dict}: Name of dimension and total expected.
    """
    for dim, expected in expected_totals.items():
        print(f"{dim}...")
        found = spark.read.parquet(path.join(output_folder, dim)).count()    
        if not (found == expected):
            raise ValueError('Unexpected total from country dimension. Expected: {} / Found: {}', expected, found) 
        else:
            print(f"Ok. Found {found} records.")

if __name__ == '__main__':
    spark = get_spark_session() 
    print("Checking lookup dims...")
    check_lookup_labels(spark, {'country': 289, 'port': 358, 'mode': 4, 'visa': 3, 'us_demographic': 49, 'immigration': 3096313})
    print("Done.")