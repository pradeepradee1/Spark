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

#creating schema for reading the json file

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

#Ans 1 - number of partitions
users_df.rdd.getNumPartitions()

#Ans 2a - Count of records
users_df.count()

#extracting columns from nested json and creating views

users_df.withColumn("user_street",col("user_address.street")) \
.withColumn("user_city",col("user_address.city")) \
.withColumn("user_state", col("user_address.state")) \
.withColumn("user_postal_code", col("user_address.postal_code")) \
.withColumn("num_phn_numbers", size(col("user_phone_numbers"))).createOrReplaceTempView("users_vw")

#Ans 2b - finding distinct user count for New York State
spark.sql("""select count(distinct user_id) as user_cnt from users_vw where user_state='New York'""")

#ans 2c - State with max postal codes
spark.sql("""select user_state,count(distinct user_postal_Code) as postal_cnt from users_vw group by user_state order by postal_cnt desc limit 1""")

#ans 2d - City with maximum users
spark.sql("""select user_city,count(distinct user_id) as user_cnt from users_vw where user_city is not null group by user_city order by user_cnt desc limit 1""")

#ans 2e - users having email domain as bizjournals.com
spark.sql("""select count(distinct user_id) as user_cnt from users_vw
where user_email like '%bizjournals.com'""")

#ans 2f - users with 4 phone numbers
spark.sql("select count(distinct user_id) as user_cnt from users_vw where num_phn_numbers=4")

#ans2g - users with no phone number
spark.sql("""select count(distinct user_id) as user_cnt from users_vw
where user_phone_numbers is null""")

spark.stop()
