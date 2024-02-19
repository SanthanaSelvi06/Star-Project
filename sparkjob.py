from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col, concat_ws, round, current_date, lit



class SparkTransform:
    def __init__(self, source_path, config_file_path, deltatablepath):
        self.source_path = source_path
        self.spark = SparkSession.builder.appName("TransformationJob").config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0").config("spark.sql.extensions",    "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
        self.spark.sparkContext.addPyFile("s3://santhanaselvi-landingbucket-batch01/delta-core_2.12-1.2.0.jar")
        self.config_file_path = config_file_path
        self.deltatablepath = deltatablepath

    def read_config(self):
        config_df = self.spark.read.json(self.config_file_path, multiLine=True)
        self.destination = config_df.select("destination_bucket").head()[0]
        self.transformations = config_df.select("transformations").first()[0].asDict()
        self.source_buckets = [self.source_path]
        self.partitions = config_df.select("partition_columns").first()[0]
        self.filename=self.source_path.split('/')[-1]
        print(self.filename)

    def transform_data(self):
        for source_bucket in self.source_buckets:
            print(f"Processing bucket: {source_bucket}")
            df = self.spark.read.parquet(source_bucket)

            for column, transformation in self.transformations.items():
                if column in df.columns:
                    if transformation == "sha2":
                        name = "masked_" + column
                        df = df.withColumn(name, sha2(col(column), 256))
                    elif "convert to decimal with" in transformation:
                        precision = int(transformation.split(" ")[-2])
                        df = df.withColumn(column, round(col(column).cast(f"decimal(10,{precision})"), precision))
                    elif transformation == "Convert to a comma-separated string":
                        df = df.withColumn(column, concat_ws(",", col(column)))

            print(f"Sample of Transformed DataFrame from {source_bucket}:")
            df.show(5, truncate=False)

            file_name = source_bucket.split("/")[-1].split(".")[0]
            output_path = f"{self.destination}/{file_name}_transformed"
            self.source_df = output_path + "/"
            df.write.partitionBy(*self.partitions).parquet(output_path, mode="overwrite")
            print(self.source_df)

    def check_delta_table(self):
        try:
            df = self.spark.read.format("delta").load(self.deltatablepath)
            df.show(5)
            return True
        except Exception as e:
            print(e)
            return False

    def update_delta_table(self):
        if self.filename == "actives.parquet":
            actives_parquet_df = self.spark.read.parquet(self.source_df)
            actives_column = ["advertising_id", "user_id", "masked_advertising_id", "masked_user_id"]
            actives_df = actives_parquet_df.select(actives_column)
            actives_df = actives_df.withColumn("start_date", current_date())
            actives_df = actives_df.withColumn("end_date", lit("0/0/0000"))
            from delta.tables import DeltaTable
            
            if self.check_delta_table():
                print("before reading delta")
                delta_table = DeltaTable.forPath(self.spark, self.deltatablepath)
                delta_df = delta_table.toDF()
                print("Existing Delta table:")
                delta_df.show(10)

                joined_df = actives_df.join(delta_df, "advertising_id", "left_outer").select(
                    actives_df["*"],
                    delta_df.advertising_id.alias("delta_advertising_id"),
                    delta_df.user_id.alias("delta_user_id"),
                    delta_df.masked_advertising_id.alias("delta_masked_advertising_id"),
                    delta_df.masked_user_id.alias("delta_masked_user_id"),
                    delta_df.start_date.alias("delta_start_date"),
                    delta_df.end_date.alias("delta_end_date")
                )

                print("Joined dataframe:")
                joined_df.show()

                filter_df = joined_df.filter((joined_df["delta_advertising_id"].isNull()) |
                                              ((joined_df["user_id"] != joined_df["delta_user_id"]) & (joined_df["delta_end_date"] == "0/0/0000")))

                print("Filter dataframe:")
                filter_df.show()

                merge_df = filter_df.withColumn("mergekey", filter_df["advertising_id"])

                print("Merge dataframe:")
                merge_df.show()

                dummy_df = merge_df.filter((merge_df["advertising_id"] == merge_df["delta_advertising_id"]) & (merge_df["user_id"] != merge_df["delta_user_id"])) \
                    .withColumn("mergekey", lit(None))

                print("Dummy dataframe:")
                dummy_df.show()

                scd_df = merge_df.union(dummy_df)

                print("SCD dataframe:")
                scd_df.show()

                delta_table.alias("delta").merge(
                    source=scd_df.alias("source"),
                    condition="delta.advertising_id = source.mergekey"
                ).whenMatchedUpdate(
                    set={
                        "end_date": str("current_date"),
                        "flag_active": lit(False)
                    }
                ).whenNotMatchedInsert(
                    values={
                        "advertising_id": "source.advertising_id",
                        "user_id": "source.user_id",
                        "masked_advertising_id": "source.masked_advertising_id",
                        "masked_user_id": "source.masked_user_id",
                        "start_date": "current_date",
                        "end_date": lit("0/0/0000"),
                        "flag_active": lit(True)
                    }
                ).execute()

                print("Updated Delta table:")
                delta_table.toDF().show(10)
                delta_df = delta_table.toDF()
                delta_df.write.format("delta").mode("overwrite").save(self.deltatablepath)
                print("After saving")

            else:
                deltacolumns = ["advertising_id", "user_id", "masked_advertising_id", "masked_user_id", "start_date", "end_date"]
                delta_df = actives_df
                delta_df = delta_df.withColumn("flag_active", lit(True))
                print("Sample delta table output:")
                delta_df.show(10)
                delta_df.write.format("delta").mode("overwrite").save(self.deltatablepath)

    def stop_spark(self):
        self.spark.stop()


source_path = SparkContext.getOrCreate().getConf().get('spark.sourcepath', 'default_value')
config_file_path = "s3://santhanaselvi-landingbucket-batch01/app.json"
deltatablepath = "s3://santhanaselvi-stagingbucket-batch01/deltatable/"

obj = SparkTransform(source_path, config_file_path, deltatablepath)
obj.read_config()
obj.transform_data()
obj.update_delta_table()
obj.stop_spark()