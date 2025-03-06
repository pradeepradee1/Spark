from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,size

spark = SparkSession. \
    builder. \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", "/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

# creating schema for reading the json file

users_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("user_first_name", StringType(), nullable=False),
    StructField("user_last_name", StringType(), nullable=False),
    StructField("user_email", StringType(), nullable=False),
    StructField("user_gender", StringType(), nullable=False),
    StructField("user_phone_numbers", ArrayType(StringType()), nullable=True),
    StructField("user_address", StructType([
        StructField("street", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("state", StringType(), nullable=False),
        StructField("postal_code", StringType(), nullable=False),
    ]), nullable=False)
])

#reading the files
users_df = spark.read \
.format("json") \
.schema(users_schema) \
.load("/public/sms/users/")

users_df.withColumn("user_street",col("user_address.street")) \
.withColumn("user_city",col("user_address.city")) \
.withColumn("user_state", col("user_address.state")) \
.withColumn("user_postal_code", col("user_address.postal_code")) \
.withColumn("num_phn_numbers", size(col("user_phone_numbers"))).createOrReplaceTempView("users_vw")

#Ans 5 state-wise gender count
users_final_df = spark.sql("""select user_state,
            sum(Male_cnt) as Male, 
            sum(Female_cnt) as Female
            from
            (select user_state, 
            case when user_gender='Male' then count(user_id) end as Male_cnt,
            case when user_gender='Female' then count(user_id) end as Female_cnt 
            from users_vw
            where user_state is not null and user_phone_numbers is not null
            group by user_state,user_gender)
            group by user_state
            order by user_state""")

users_final_df.write.format("parquet").mode("overwrite").option("path","/user/itv006753/pivot_assignment_result").save()

spark.stop()
