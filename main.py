import logging
from datetime import datetime
from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql.functions import when, col

os.environ['HADOOP_HOME'] = "C:\\winutils"
# Date time and timestamp functions
now = datetime.now()
today_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
print(today_timestamp)
today = now.strftime("%m-%d-%Y")
print(today)

# logging.basicConfig(filename="error_check_log.txt", level=logging.INFO,
#                     format="%(pastime)s %(message)s", filemode="w")

print("Error & Duplicate check process started:", today_timestamp)
# mysql jdbc connection variables
apar_url = "jdbc:mysql://phpdemo03.kcspl.in/aws_poc?useSSL=false"
jdbcUsername = "admin"
jdbcPassword = "Krish@123"
mode = "append"
ingested_data = "ingested_data"
full_data = "all_data"
properties = {"driver": 'com.mysql.jdbc.Driver'}

# starting SparkSession
spark = SparkSession.builder.config("spark.jars",
                                    '/D:/mysql-connector-java-8.0.11.jar') \
    .appName("Error_Check").getOrCreate()

# Reading ingested_data and all_data tables from mysql as dataframes
print("Reading data from mysql started:{}".format(datetime.utcnow()))
ingested_data = spark.read.format("jdbc").option("url", apar_url).option("driver", "com.mysql.cj.jdbc.Driver").option(
    "dbtable", ingested_data).option("user", jdbcUsername).option("password", jdbcPassword).option("mode", mode).option(
    "properties", properties).option("numPartitions",
                                     8).load()
print("ingested data records:", ingested_data.count())
full_data = spark.read.format("jdbc").option("url", apar_url).option("mode", mode).option("properties",
                                                                                          properties).option("driver",
                                                                                                             "com.mysql.cj.jdbc.Driver").option(
    "dbtable", full_data).option("user", jdbcUsername).option("password", jdbcPassword).option("numPartitions",
                                                                                               8).load()
print("all data records:", full_data.count())

logger1 = logging.getLogger('py4j')
logger1.info("Reading data from mysql completed")
print("Reading data from mysql completed:{}".format(datetime.utcnow()))

raw_data = ingested_data.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in ingested_data.columns])

all_data = full_data.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in raw_data.columns])

# changing data format of invoice_date and invoice_due_date
raw_data_date1 = raw_data.withColumn("invoice_date",
                                     (f.to_date("invoice_date", "yyyyMMdd").alias("invoice_date"))).withColumn(
    "invoice_due_date", (f.to_date("invoice_due_date", "yyyyMMdd").alias("invoice_due_date")))
raw_data_date2 = raw_data_date1.withColumn("invoice_date", (
    f.date_format("invoice_date", "MM/dd/yyyy").alias("invoice_date"))).withColumn("invoice_due_date", (
    f.date_format("invoice_due_date", "MM/dd/yyyy").alias("invoice_due_date")))

logger2 = logging.getLogger('py4j')
logger2.info("changing date fromat completed")
print("changing date fromat completed:{}".format(datetime.utcnow()))

print("Error check started:{}".format(datetime.utcnow()))
mandatory_field_check_not_null = raw_data_date2.na.drop(how="any",
                                                        subset=["scac", "invoice_number", "mode", "invoice_date",
                                                                "invoice_total_amount", "invoice_due_date"])
mandatory_field_null = raw_data_date2.filter(
    "invoice_number is NULL or scac is NULL or mode is NULL or invoice_date is NULL or invoice_total_amount is NULL or invoice_due_date is NULL")

mandatory_field_check_not_null.createOrReplaceTempView('mandatory_field_check_not_null')

# adding error remarks
conversions = {
    'scac': lambda c: f.col(c),
    'invoice_number': lambda c: f.col(c),
    'mode': lambda c: f.col(c),
    'invoice_date': lambda c: f.col(c),
    'invoice_total_amount': lambda c: f.col(c),
    'invoice_due_date': lambda c: f.col(c),

}

mandatory_field_null_error = mandatory_field_null.withColumn(
    "error_remarks",
    f.concat_ws(", ",
                *[
                    f.when(
                        v(k).isNull(),
                        f.lit(k + " is null")
                    ).otherwise(f.lit(None))
                    for k, v in conversions.items()
                ]
                )
)

logger3 = logging.getLogger('py4j')
logger3.info("Error check completed")
print("Error check completed:{}".format(datetime.utcnow()))

print("Error check based on mode started:{}".format(datetime.utcnow()))


print("Error check based on mode started:{}".format(datetime.utcnow()))
# parcel
parcel_not_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'parcel' and tracking_number is not NULL ")
parcel_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'parcel' and tracking_number is NULL ").withColumn(
    "error_remarks", f.lit('tracking_number is null'))

# Air
air_not_null = spark.sql("select * from mandatory_field_check_not_null where mode = 'air' and awb_number is not NULL ")
air_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'air' and awb_number is NULL ").withColumn(
    "error_remarks", f.lit('awb_number is null'))

# customs clearence
customs_not_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'customs clearence' and master_bol is not NULL and house_bol is not NULL")
customs_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'customs clearence' and (master_bol is NULL and house_bol is NULL)").withColumn(
    "error_remarks", f.lit('master_bol/house_bol is null'))

# LTL
ltl_not_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'ltl' and pro_number is not NULL and bol_number is not NULL and class is not NULL ")
ltl_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'ltl' and (pro_number is NULL or bol_number is NULL or class is NULL) ").withColumn(
    "error_remarks", f.lit('pro_number/bol_number/class is null'))

# FTL
ftl_not_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'ftl' and pro_number is not NULL and bol_number is not NULL")
ftl_null = spark.sql(
    "select * from mandatory_field_check_not_null where mode = 'ftl' and (pro_number is NULL or bol_number is NULL) ").withColumn(
    "error_remarks", f.lit('pro_number/bol_number is null'))

# other than above
others = spark.sql(
    "select * from mandatory_field_check_not_null where mode NOT IN('parcel','air','customs clearence','ltl','ftl') ")

# union of all modes
union_ltl_ftl = ftl_not_null.union(ltl_not_null)
union_cus_air = customs_not_null.union(air_not_null)
union_cus_air_par = parcel_not_null.union(union_cus_air)
union_ltl_ftl_others = union_ltl_ftl.union(others)
final_mandatory_field_check = union_cus_air_par.union(union_ltl_ftl_others)

# union of all modes for null records
union_ltl_ftl_null = ftl_null.union(ltl_null)
union_cus_air_null = customs_null.union(air_null)
union_cus_air_par_null = parcel_null.union(union_cus_air_null)
final_field_check_null = union_cus_air_par_null.union(union_ltl_ftl_null)
final_mandatory_field_check_null = final_field_check_null.union(mandatory_field_null_error)

logger4 = logging.getLogger('py4j')
logger4.info("Error check based on mode complete")
print("Error check based on mode completed:{}".format(datetime.utcnow()))


# Data type Validation Check
print("Data type validation started:{}".format(datetime.utcnow()))
#
# conversions = [
#
#     'invoice_due_date',
#     'weight',
#     'fuel_surcharge',
#     'base_freight_rate',
#     'prepull_fee',
#     'scac',
#     'yard_storage_fee',
#     'invoice_total_amount',
#     'inside_pu',
#     'lift_gate_pu',
#     'holiday_pu',
#     'weekend_pu',
#     'non_business_hour_pu',
#     'sorting_segregation',
#     'marking_tagging',
#     'trade_show_pu',
#     'appointment_required',
#     'nyc_metro',
#     'pallet_count',
#     'other_accessorials',
#     'large_package_surcharge',
#     'additional_handling_charge',
#     'residential_surcharge',
#     'signature_surcharge',
#     'correction_surcharge',
#     'delivery_surcharge',
#     'adjustment',
#     'next_day_surcharge',
#     'zone_adjustment_charge',
#     'remote_surcharge',
#     'return_surcharge',
#     'second_day_surcharge',
#     'credits',
#     'linehaul',
#     'pallet_jack',
#     'residential_pu',
#     'fuel',
#     'detention',
#     'toll_fee',
#     'layover',
#     'stop_off',
#     'driver_assist',
#     'weight_increase',
#     'ams',
#     'bol_fee',
#     'bonded_fee',
#     'cancellation_charge',
#     'chassis_charges',
#     'congestion_surcharge',
#     'customs_clearance_fee',
#     'delivery_order_fee',
#     'demurrage',
#     'destination_fees',
#     'diversion_charges',
#     'drop_and_hook_fee',
#     'handling_fee',
#     'hazardous',
#     'pick_up_charge',
#     'redelivery_fee',
#     'reefer_surcharge',
#     'terminal_handling_charge',
#     'wait_time_fee',
#     'duty_hmf_mpf_fee',
#     'scale_ticket',
#     'gri',
#     'peak_season_surcharge',
#     'prism_id',
#     'isf_fee',
#     'origin_charge',
#     'per_diem_charge',
#     'pier_pass',
#     'cfs_storage_fee',
#     'telex_release_fee',
#     'additional_line_fee',
#     'lumper',
#     'limited_access_type',
#     'discount'
#
# ]

datatype_validation = final_mandatory_field_check.withColumn(
    "error_remarks",
    f.concat_ws(", ", )

)
logger5 = logging.getLogger('py4j')
logger5.info("Data type validation completed")
print("Data type validation completed:{}".format(datetime.utcnow()))

logger5 = logging.getLogger('py4j')
logger5.info("Data type validation completed")
print("Data type validation completed:{}".format(datetime.utcnow()))

# Reading validated and invalid datatypes into dataframes
validated_datatype = datatype_validation.where("error_remarks = ''")
invalid_datatype = datatype_validation.where(col('error_remarks').like("%not%"))

# removing error_remarks column in valid datatype records
validated_datatype1 = validated_datatype.drop('error_remarks')

# union of error data
union_error_data = invalid_datatype.union(final_mandatory_field_check_null).withColumn("updatedAt",
                                                                                       f.lit(today_timestamp))
print("error data records:", union_error_data.count())


print("Duplicate check process started:{}".format(datetime.utcnow()))
non_duplicate_data1 = validated_datatype1.join(all_data, [validated_datatype1.invoice_number == all_data.invoice_number,
                                                          validated_datatype1.scac == all_data.scac,
                                                          validated_datatype1.invoice_total_amount == all_data.invoice_total_amount],
                                               how='left_anti').withColumn("updatedAt", f.lit(today_timestamp))
final_valid_data = non_duplicate_data1.dropDuplicates(['scac', 'invoice_number', 'invoice_total_amount'])

# reading duplicate records into a dataframe
duplicate_data = validated_datatype1.exceptAll(final_valid_data).withColumn("updatedAt", f.lit(today_timestamp))

#print("duplicate data records:", duplicate_data.count())
print("final valid data records:", final_valid_data.count())

logger6 = logging.getLogger('py4j')
logger6.info("Duplicate check process completed")

print("Duplicate check process completed:{}".format(datetime.utcnow()))


print("writing data into msql tables started:{}".format(datetime.utcnow()))
# Declared variables for storing table names
error_table = "error_data"
duplicate_table = "duplicate_data"
valid_table = "valid_data"
all_data = "all_data"

# dropping id column from all dataframes to write mysql table
ingested_data_with_updatedtime = ingested_data.withColumn("updatedAt", f.lit(today_timestamp))
union_error_data1 = union_error_data.drop('id')
duplicate_data1 = duplicate_data.drop('id')
final_valid_data1 = final_valid_data.drop('id')
ingested_data_with_updatedtime1 = ingested_data_with_updatedtime.drop('id')

# Writing data into mysql tables
union_error_data1.write.format("jdbc").mode("overwrite") \
    .option("mode", mode) \
    .option("properties", properties) \
    .option("url", apar_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", error_table) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword)\
    .option("batchsize", "1000") \
    .save()

duplicate_data.show()

# duplicate_data.write.format("jdbc").mode("overwrite") \
#     .option("mode", mode) \
#     .option("properties", properties) \
#     .option("url", apar_url) \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", duplicate_table) \
#     .option("user", jdbcUsername) \
#     .option("password", jdbcPassword) \
#     .option("batchsize", "1000")\
#     .save()

final_valid_data1.write.format("jdbc").mode("overwrite") \
    .option("mode", mode) \
    .option("properties", properties) \
    .option("url", apar_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", valid_table) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("batchsize", "1000") \
    .save()

ingested_data_with_updatedtime1.write.format("jdbc").mode("overwrite") \
    .option("mode", mode) \
    .option("properties", properties) \
    .option("url", apar_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", all_data) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("batchsize", "100") \
    .save()

logger7 = logging.getLogger('py4j')
logger7.info("writing data into msql completed")
print("writing data into msql tables completed:{}".format(datetime.utcnow()))

# # truncating ingested_data
# print("truncating ingested data table started:{}".format(datetime.utcnow()))
# ingested_data.createOrReplaceTempView('empty')
# df_empty = spark.sql("select * from empty where id ='sdjhrbcgrj'")
# table1 = "ingested_data"
#
# df_empty.write.mode('overwrite').format("jdbc")\
#     .option("url", apar_url)\
#     .option("driver", "com.mysql.cj.jdbc.Driver")\
#     .option("dbtable", table1)\
#     .mode("overwrite")\
#     .option("mode", mode)\
#     .option("properties", properties)\
#     .option("user",jdbcUsername)\
#     .option("password", jdbcPassword)\
#     .option("truncate", "true")\
#     .save()

logger8 = logging.getLogger('py4j')
logger8.info("truncating ingested data completed")
print("truncating ingested data table completed:{}".format(datetime.utcnow()))

print("Error and duplicate check process completed", datetime.now())


#

























