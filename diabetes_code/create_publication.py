import os
import sys
import time

import pyspark
import timeit
import toml
import findspark
findspark.init(spark_home=r"C:\Program Files\spark-3.1.1-bin-hadoop2.7")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import dataframe as DF

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 2)
spark.sparkContext.setLogLevel('WARN')

log_folder = f"./reports/logs/"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

import logging

logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s - %(levelname)s -- %(filename)s:%(funcName)20s():%(lineno)s -- %(message)s',
    handlers=[
        logging.FileHandler(f"./reports/logs/{time.strftime('%Y-%m-%d_%H-%M-%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

from diabetes_code.params import params
from diabetes_code.utilities.data_connections import get_df_from_SQL, write_df_to_SQL
from diabetes_code.utilities.field_definitions import aggregate_fields
from diabetes_code.utilities.processing_steps import (
    read_and_prepare_data,
    assign_record_scores,
    identify_best_record,
    create_golden_record_table,
    enrich_golden_record,
    produce_aggregates
)

def main():
    logger.info(params)
    # Use the params to specify the golden record SQL address: E.g., "dbo.202021_E3_golden_record"
    golden_record_sql = f"dbo.golden_record_{params['publication_year']}_{params['publication_version']}"

    logger.info("Step 1: Import all the input data from MSSQL database")
    (nda_demo, nda_bmi, nda_bp, nda_chol, nda_hba1c, ccg_map, hes_diabetes, nda_drug, imd_scores) = read_and_prepare_data(params=params)


    logger.info("Step 2: Get scores for each of the records")
    record_scores = assign_record_scores(
                            nda_demo=nda_demo,
                            nda_bmi=nda_bmi,
                            nda_bp=nda_bp,
                            nda_chol=nda_chol,
                            nda_hba1c=nda_hba1c,
                            ccg_map=ccg_map,
                            hes_diabetes = hes_diabetes,
                            nda_drug = nda_drug)


    logger.info("Step 3: Identify the best record for each person")
    best_record = identify_best_record(record_scores=record_scores)


    logger.info("Step 4: Use the best record to cut down the record_scores table, creating the golden record")
    golden_record = create_golden_record_table(
                        record_scores=record_scores,
                        best_record=best_record)


    logger.info("Step 5: Derive additional fields for the golden record table")
    golden_record = enrich_golden_record(golden_record=golden_record,
                                            hes_diabetes=hes_diabetes,
                                            imd_scores=imd_scores)



    logger.info("Step 6: Output the golden record table to SQL so we can avoid recalculating next time")
    write_df_to_SQL(df_to_write = golden_record,
                    target_table = golden_record_sql,
                    mode = 'overwrite',
                    database = params['work_db'])


    logger.info("Step 7: After attain the golden_record (either by saved table or generate new one), we build final table")
    final_table = produce_aggregates(golden_record)


    logger.info("Step 8: Export these aggregates to SQL")
    final_table_sql = f"dbo.final_table_{params['publication_year']}_{params['publication_version']}"

    write_df_to_SQL(df_to_write = final_table,
                        target_table = final_table_sql,
                        mode = 'overwrite',
                        database = params['work_db'])

    logger.info(f"The first 200 records of final table with the shape {final_table.count(), len(final_table.columns)} is shown below.")
    logger.info(f".\n, {final_table.limit(200).toPandas()}")

if __name__ == "__main__":
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_publication: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.")
