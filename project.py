# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Kanika Jindal"
__email__ = ""

from pyspark.sql import SparkSession
import logging
import threading

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class SessionNotCreatedException(Exception):
    def __str__(self):
        return('***   Session Not Created Exception   ***')


class SparkEssentials():
    """
        Attributes : None
        Methods : 
            get_spark_session :- Returns spark session with hive support
    """
    @staticmethod
    def get_spark_session():
        s3a_connection_maximun = 400
        return SparkSession.builder.config("metastore.catalog.default","hive").enableHiveSupport().getOrCreate()


class CsvFileReader():
    """
        Attributes :
            file_path = path of file to be read
        Methods:
            get_data_frame :- Returns dataframe of the csv file
    """
    def __init__(self,file_path):
        self.file_path = file_path
        self.spark_session = SparkEssentials.get_spark_session()

    def get_data_frame(self):
        dataframe = self.spark_session.read.format("csv").option("header", "true").load(self.file_path)
        return(dataframe)


class HiveConnection():
    """
        Attributes : None
        Methods :
            get_data_frame :- Returns dataframe from the query passed
            set_data :- Create a hive table from dataframe
                        arguments - dataframe, table_name, mode(optional)
    """
    @staticmethod
    def get_data_frame(query):
        spark = SparkEssentials.get_spark_session()
        df = spark.sql(query)
        return(df)

    @staticmethod
    def set_data(df,table_name,**kwargs):
        spark = SparkEssentials.get_spark_session()
        if spark == None:
            raise SessionNotCreatedException()
        try:
            mode = kwargs["mode"]
        except:
            mode = None
        if mode == "overwrite":
            df.write.format("orc").mode("overwrite").saveAsTable(table_name)
        elif mode == "append":
            df.write.format("orc").mode("append").saveAsTable(table_name)
        else:
            df.write.format("orc").saveAsTable(table_name)
    
    @staticmethod
    def execute_query(query):
        spark = SparkEssentials.get_spark_session()
        spark.sql(query)


class UpdateData():
    """
        Attributes : 
            staging_table = temporary table used for calculations
            final_table = final table consisting of data
    """
    def __init__(self,staging_table,final_table,join_columns):
        self.staging_table = staging_table
        self.final_table = final_table
    
    def load_into_staging(self,path,table_name):
         HiveConnection.set_data(CsvFileReader(path).get_data_frame(),table_name=table_name,mode="overwrite")
    
    def apply_scd2(self):
        staging = HiveConnection.get_data_frame("SELECT * FROM {}".format(self.staging_table))
        final_table = HiveConnection.get_data_frame("SELECT * FROM {} where is_active = 1".format(self.final_table))
        new_rows = staging.join(final_table,self.join_columns,how="left_anti")
        new_rows = new_rows.withColumn("is_active", 1)
        old_rows = final_table.join(new_rows,self.join_columns,how="left_anti")
        final_df = new_rows.unionByName(old_rows)
        HiveConnection.set_data(final_df,table_name=staging_table,how="overwrite")
        HiveConnection.execute_query("INSERT INTO {} SELECT * FROM {} WHERE is_active = 0".format(self.staging_table, self.final_table))
        HiveConnection.execute_query("INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(self.final_table, self.staging_table))


# Unblocked and concurent reading and processing of file through different threads
if __name__== "__main__":
    logging.info("Starting to Process files ")
    path_to_file_1 = "file_name.csv"
    path_to_file_2 = "file_name.csv"
    thread_1 = threading.Thread(target=UpdateData().load_into_staging,args=([path_to_file_1,"db.t1"]))
    thread_1.start()
    thread_2 = threading.Thread(target=UpdateData().load_into_staging,args=([path_to_file_2,"db.t2"]))
    thread_2.start()
    thread_2.join()
    thread_2.join()
    logging.info("Processing of file completed")


