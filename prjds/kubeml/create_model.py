from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import mean

# Initialize Spark Session
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Read") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the data
#df = spark.read.csv("insurance.csv", inferSchema=True, header=True)
# Define SQL query
query = "SELECT * FROM usuk30.insurance"
df = spark.sql(query)
df.printSchema()

mean_val = df.select(mean(df['age'])).collect()[0][0]
df_filled = df.na.fill({'age': mean_val})
# String indexing for categorical features
indexers = [
    StringIndexer(inputCol=column, outputCol=column + "_indexed").fit(df_filled)
    for column in ["sex", "smoker", "region"]
]

# Vector assembler to combine feature columns into a single vector column
#assembler = VectorAssembler(inputCols=["age", "bmi", "children"] + [column + "_indexed" for column in ["sex", "smoker", "region"]], outputCol="features")
assembler = VectorAssembler(
    inputCols=["age", "bmi", "children"] + [column + "_indexed" for column in ["sex", "smoker", "region"]],
    outputCol="features",
    handleInvalid="skip"  # Skip rows with null values
)

# Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="expenses")

# Pipeline: StringIndexer, VectorAssembler, and LinearRegression
pipeline = Pipeline(stages=indexers + [assembler, lr])

# CrossValidator with ParamGrid
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(labelCol="expenses"),
                          numFolds=3)

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(df)

# Save the best model
# Save the best model to the mounted volume
cvModel.bestModel.save("/opt/spark-app/models/insurance_model")

# Stop the SparkSession
spark.stop()

