from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Imputer
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer






# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Read") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/warehouse/tablespace/managed/hive/") \
    .config("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()


query = "SELECT * FROM default.live_insurance_claim"
data = spark.sql(query)
data.show(5)

data.printSchema()

uni_catcols = ["PARENT1","MSTATUS", "GENDER", "EDUCATION", "OCCUPATION",
"CAR_USE", "CAR_TYPE","RED_CAR", "REVOKED", "URBANICITY"]

#columns_to_convert = ['INCOME', 'HOME_VAL', 'BLUEBOOK', 'OLDCLAIM', 'CLM_AMT']

columns_to_convert = ["AGE", "BLUEBOOK", "CAR_AGE","CLAIM_FLAG", "CLM_AMT", "CLM_FREQ", "HOMEKIDS", "HOME_VAL", "ID", "INCOME",
"KIDSDRIV", "MVR_PTS", "OLDCLAIM", "TIF", "TRAVTIME", "YOJ"]

# Applying transformations to each colum
for column_name in columns_to_convert:
    # Remove $ and , from the column data
    data = data.withColumn(column_name, regexp_replace(col(column_name), '[\$,]', ''))
    # Cast the column to float
    data = data.withColumn(column_name, col(column_name).cast('float'))


#indexers = [StringIndexer(inputCol=col, outputCol=col+"_indexed").fit(data) for col in uni_catcols]
#pipeline = Pipeline(stages=indexers)
#data = pipeline.fit(data).transform(data)

from pyspark.ml import Pipeline

# Assuming uni_catcols contains the names of your categorical columns
indexers = [StringIndexer(inputCol=col, outputCol=col+"_indexed").fit(data) for col in uni_catcols]

# Create a Pipeline with the list of StringIndexers
pipeline = Pipeline(stages=indexers)

# Fit the Pipeline to your data and transform it
data = pipeline.fit(data).transform(data)

# At this point, your data has been transformed with StringIndexers applied to each categorical column



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
                           handleInvalid = "keep")


final_data = assembler.transform(data)

final_data.show(5)

df = final_data.select('features')


    # Load the saved model
model_path = "/models/insurance_model"  # Ensure this is the correct path where the model was saved
loaded_model = LogisticRegressionModel.load(model_path)

# Perform prediction
predicted = loaded_model.transform(df)

predicted = predicted.select('prediction')

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession

# Add a row index to both DataFrames
final_data_with_index = data.withColumn("row_index", monotonically_increasing_id())
predicted_with_index = predicted.withColumn("row_index", monotonically_increasing_id())

# Join the DataFrames on the row index
# Here we're transferring the 'prediction' column from 'predicted' to 'final_data'
final_data_joined = final_data_with_index.join(predicted_with_index, "row_index") \
    .select(final_data_with_index["*"], predicted_with_index["prediction"].alias("PredictedClaimFlag"))

# Drop the row_index column if not needed anymore
final_data_joined = final_data_joined.drop("row_index")

# Show result (or proceed with further operations)
final_data_joined.show(2)

# Save predictions back to Hive (Correct the DataFrame reference and table name)
final_data_joined.write.mode('overwrite').saveAsTable("carinsuranceclaimspredicted")
