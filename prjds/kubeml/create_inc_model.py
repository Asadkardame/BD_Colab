from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Imputer
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

# spark = SparkSession.builder.master("local[*]").getOrCreate()
appName= "SparkwithMlib"
master= "local"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Read") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/warehouse/tablespace/managed/hive/") \
    .config("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

query = "SELECT * FROM project1db.carinsuranceclaims"
data = spark.sql(query)
data.show(5)

data.printSchema()

uni_catcols = ["PARENT1","MSTATUS", "GENDER", "EDUCATION", "OCCUPATION",
"CAR_USE", "CAR_TYPE","RED_CAR", "REVOKED", "URBANICITY"]

columns_to_convert = ['INCOME', 'HOME_VAL', 'BLUEBOOK', 'OLDCLAIM', 'CLM_AMT']

# Applying transformations to each colum
for column_name in columns_to_convert:
    # Remove $ and , from the column data
    data = data.withColumn(column_name, regexp_replace(col(column_name), '[\$,]', ''))
    # Cast the column to float
    data = data.withColumn(column_name, col(column_name).cast('float'))

# for column_name in uni_catcols:
#     data = data.withColumn(column_name, col(column_name).cast('string'))

indexers = [StringIndexer(inputCol=col, outputCol=col+"_indexed").fit(data) for col in uni_catcols]
pipeline = Pipeline(stages=indexers)
data = pipeline.fit(data).transform(data)

imputer = Imputer(inputCols=["AGE", "HOME_VAL", "YOJ","INCOME", "CAR_AGE"], outputCols=["AGE", "HOME_VAL", "YOJ","INCOME", "CAR_AGE"], strategy="median")

model = imputer.fit(data)

data = model.transform(data)

assembler = VectorAssembler(inputCols = ['KIDSDRIV', 'AGE', 'HOMEKIDS', 'YOJ','INCOME',
                                        'HOME_VAL','TRAVTIME','BLUEBOOK', 'TIF', 'OLDCLAIM',
                                        'CLM_FREQ', 'MVR_PTS', 'CAR_AGE','PARENT1_indexed',
                                        'MSTATUS_indexed', 'GENDER_indexed','EDUCATION_indexed',
                                        'CAR_USE_indexed','CAR_TYPE_indexed', 'RED_CAR_indexed',
                                        'REVOKED_indexed'],
                           outputCol='features',
                           handleInvalid = "skip")




final_data = assembler.transform(data)

final_data.show(5)

df = final_data.select(['features', 'CLAIM_FLAG'])
df =  df.withColumnRenamed('CLAIM_FLAG', "label")
df.show(5)

df = df.withColumn("label", df["label"].cast(IntegerType()))

train, test = df.randomSplit([0.7, 0.3], seed = 123)

from pyspark.ml.classification import LogisticRegression

log_reg = LogisticRegression(maxIter=10).fit(train)

predictions = log_reg.transform(test)

predictions.select('label', 'prediction').show(10)

claim_eval = BinaryClassificationEvaluator(rawPredictionCol = "prediction", labelCol = "label" )
auc = claim_eval.evaluate(predictions)

auc

log_reg.save("/opt/spark-app/models/insurance_model")
#log_reg.save("carinsurance_model")

