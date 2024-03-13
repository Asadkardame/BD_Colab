from pyspark.sql import SparkSession
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import udf, col,f,regexp_replace
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
#findspark.init()

## Read Data

# spark = SparkSession.builder.master("local[*]").getOrCreate()
appName= "SparkwithMlib"
master= "local"

spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()

query = "SELECT * FROM project1db.carinsuranceclaims"
data = spark.sql(query)
data.show(5)

data.printSchema()

# data1.withColumn("INCOME", regexp_replace(data1['INCOME'], '\$',''))
#
# data.select(regexp_replace('INCOME', '\$',''))
#
#
# data1.select("INCOME", f.translate(f.col("INCOME"), "$", "")).alias("INCOME_NEW").show()
#
# from pyspark.sql import functions as F
# from pyspark.sql import functions as T
# from pyspark.sql.functions import udf


# replacesign1 = F.udf(lambda x: x.replace("$", ""), T.StringType())
#
# data1.withColumn("INCOME", replacesign1(F.col("INCOME")))


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




final_data = assembler.fit(data).transform(data)

final_data.show(5)

df = final_data.select(['features', 'CLAIM_FLAG'])
df =  df.withColumnRenamed('CLAIM_FLAG', "label")
df.show(5)

df = df.withColumn("label", df["label"].cast(IntegerType()))

train, test = df.randomSplit([0.7, 0.3], seed = 123)

## Models


### Logistic Regression

from pyspark.ml.classification import LogisticRegression

log_reg = LogisticRegression(maxIter=10).fit(train)

# lr_summary = log_reg.summary
#
# # Overall accuracy of model
# lr_summary.accuracy
#
# # Area under ROC
# lr_summary.areaUnderROC
#
# # Precision of both classes
# print(lr_summary.precisionByLabel)
#
# # Recall of both classes
# print(lr_summary.recallByLabel)

# Predictions
predictions = log_reg.transform(test)

predictions.select('label', 'prediction').show(10)

# from sklearn.metrics import confusion_matrix
# y_true = predictions.select("label")
# y_true = y_true.toPandas()
#
# y_pred = predictions.select("prediction")
# y_pred = y_pred.toPandas()
# class_names = [1, 0]
# cnf_matrix = confusion_matrix(y_true, y_pred,labels=class_names)
# cnf_matrix




claim_eval = BinaryClassificationEvaluator(rawPredictionCol = "prediction", labelCol = "label" )
auc = claim_eval.evaluate(predictions)


### Random Forest Classifier

# from pyspark.ml.classification import RandomForestClassifier
#
# rf = RandomForestClassifier()
# rfModel = rf.fit(train)
# rfPreds = rfModel.transform(test)
#
# rfPreds.select('label', 'rawPrediction', 'prediction', 'probability').show(10)
#
# rfModel_summary = rfModel.summary
# rfModel_summary.accuracy
#
# ### Gradient-Boosted Tree Classifier
#
# from pyspark.ml.classification import GBTClassifier
#
# gbt = GBTClassifier(maxIter=10)
# gbtModel = gbt.fit(train)
# gbtPreds = gbtModel.transform(test)
#
# gbtPreds.select('label', 'rawPrediction', 'prediction', 'probability').show(20)
