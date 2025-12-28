import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

adls_path = "abfss://suraksha@hisadls.dfs.core.windows.net"

source_table = {
    "br_acxtestdone" : "acxtestdone",
    "br_doctorattendance" : "doctorattendance",
    "br_inventorymaster" : "inventorymaster",
    "br_patientsdata" : "patientsdata",
    "br_routinetests" : "routinetests"
}

def bronze_ingestion(table, source_folder):
    @dlt.table(
        name= table,
        comment= "Raw ingestion from ADLS"
    )
    def br_ing():
        df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
                .option("header", "true")\
                    .load(f"{adls_path}/{source_folder}")\
                        .withColumn("source_file", col("_metadata.file_path"))\
                            .withColumn("ingested_at", current_timestamp())

        return df
    
for table, source_folder in source_table.items():
    bronze_ingestion(table, source_folder)
