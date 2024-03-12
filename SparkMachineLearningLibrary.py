#!/usr/bin/env python
# coding: utf-8

# In[2]:


# get_ipython().system('pip install findspark')


# ## Imports

# In[1]:


# import findspark
import os
from pyspark.sql import SparkSession
#from pyspark.ml.feature import RegexTokenizer, RegexMatcher
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# In[4]:


# findspark.init()


# ## Read Data

# In[2]:


# spark = SparkSession.builder.master("local[*]").getOrCreate()
appName= "SparkwithMlib"
master= "local"

spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()


# In[4]:


# data = spark.read.csv('/content/query-hive-10926.csv', header=True, inferSchema=True)
# query = "SELECT * FROM usuk30.insurance_data"
query = "SELECT * FROM project1db.carinsuranceclaims"
data = spark.sql(query)
data.show(5)


# ## Data Cleaning

# In[5]:


data.printSchema()


# In[14]:


# for col in data.columns:
#     if 'carinsuranceclaims.' in col:
#         new_col_name = col.replace('carinsuranceclaims.', '')
#         data = data.withColumnRenamed(col, new_col_name)


# In[6]:


data.printSchema()


# In[7]:


columns_to_transform = ["home_val", "income", "bluebook", "oldclaim", "clm_amt"]

def strip_special_characters(x):
    if isinstance(x, str):
        return x.replace("$", "").replace(",", "")
    else:
        return x

udfdollarsign = udf(strip_special_characters, StringType())

for i in columns_to_transform:
    data = data.withColumn(i, udfdollarsign(data[i]))


# In[50]:


# Cast to right data type
data = data.withColumn("age", data["age"].cast(FloatType()))
data = data.withColumn("yoj", data["yoj"].cast(FloatType()))
data = data.withColumn("income", data["income"].cast(FloatType()))
data = data.withColumn("home_val", data["home_val"].cast(FloatType()))
data = data.withColumn("bluebook", data["bluebook"].cast(FloatType()))
data = data.withColumn("oldclaim", data["oldclaim"].cast(FloatType()))
data = data.withColumn("clm_amt", data["clm_amt"].cast(FloatType()))
data = data.withColumn("car_age", data["car_age"].cast(FloatType()))


# In[51]:


# Clean mstatus column
data = data.withColumn('mstatus', F.when(F.col('mstatus') == 'Married', 'Yes').
                        when(F.col('mstatus') == 'Single', 'No').
                        otherwise(F.col('mstatus')))


# In[52]:


# Clean gender column
data = data.withColumn('gender', F.when(F.col('gender') == 'Female', 'F')
                        .when(F.col('gender') == 'Male', 'M')
                        .otherwise(F.col('gender')))


# In[53]:


# Clean red_car column
data = data.withColumn('red_car', F.when(F.col('red_car') == 'No', 'no')
                        .when(F.col('red_car') == 'Yes', 'yes')
                        .otherwise(F.col('red_car')))


# In[54]:


data.groupBy('mstatus').count()


# In[55]:


data.groupBy('urbanicity').count().show()


# In[56]:


from pyspark.sql.functions import col, count, when


# In[57]:


def count_cols(columns):
    exprs = [
        count(
            when(col(c).isNull() | (col(c) == "") | col(c).contains("NULL") | col(c).contains("null"), c)
        ).alias(c)
        for c in columns
    ]
    return exprs


# In[58]:


# from pyspark.sql.functions import col, count, when
# import os
# os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

# def count_cols(columns):
#     exprs = [
#         count(
#             when(col(c).isNull() | (col(c) == "") | col(c).contains("NULL") | col(c).contains("null"), c)
#         ).alias(c)
#         for c in columns
#     ]
#     return exprs

# Apply the function to the DataFrame
null_counts = data.select(count_cols(data.columns))

# Display the results
# null_counts.show()


# In[59]:


null_counts.printSchema()


# In[60]:


null_counts


# In[61]:


# input missing values with median
#data = data.withColumn("home_val", when(F.col("home_val") == 0, None).otherwise(F.col("home_val")))

imputer = Imputer(inputCols=["age", "home_val", "yoj","income", "car_age"], outputCols=["age", "home_val", "yoj","income", "car_age"], strategy="median")

model = imputer.fit(data)

data = model.transform(data)


# In[ ]:


null_counts = data.select(count_cols(data.columns))

# Display the results
null_counts.show()


# In[18]:


# Drop columns
columns_to_drop = ["policy_number", "birth", "occupation", "urbanicity"]

# Drop the specified columns
data = data.drop(*columns_to_drop)


# ## Train | Test Sets

# ## Pre-process the Data

# In[19]:


cols = ['parent1', 'gender', 'education', 'car_use', 'car_type', 'red_car', 'revoked', 'urbanicity', 'mstatus', 'claim_flag']
# Remove specified columns from the list of column names
num_cols = [col for col in data.columns if col not in cols]

catcols = [col for col in data.columns if col not in num_cols and col != 'claim_flag']


labelcol = "claim_flag"


# In[19]:





# In[20]:


# String Index
indexer = StringIndexer(inputCols =  catcols,
                        outputCols = [col + "_indexed" for col in catcols])

data = indexer.fit(data).transform(data)


# In[21]:


data.show(5)


# In[22]:


# One hot Encode
OHE = OneHotEncoder(inputCols =['parent1_indexed', 'mstatus_indexed',
                                       'gender_indexed', 'education_indexed',
                                       'car_use_indexed', 'car_type_indexed',
                                       'red_car_indexed',
                                       'revoked_indexed'], outputCols = ['parent1_OHE', 'mstatus_OHE',
                                       'gender_OHE', 'education_OHE',
                                       'car_use_OHE', 'car_type_OHE',
                                       'red_car_OHE',
                                       'revoked_OHE'])
data = OHE.fit(data).transform(data)


# In[23]:


data.show(5)


# In[24]:


assembler = VectorAssembler(inputCols=['kidsdriv','age','homekids','yoj','income','home_val', 'travtime',
                                       'bluebook', 'tif','oldclaim', 'clm_freq', 'mvr_pts',
                                       'car_age', 'parent1_indexed', 'mstatus_indexed',
                                       'gender_indexed', 'education_indexed',
                                       'car_use_indexed', 'car_type_indexed',
                                       'red_car_indexed',
                                       'revoked_indexed', 'parent1_OHE', 'mstatus_OHE',
                                       'gender_OHE', 'education_OHE',
                                       'car_use_OHE', 'car_type_OHE',
                                       'red_car_OHE',
                                       'revoked_OHE'],
                            outputCol='features')

final_data = assembler.transform(data)


# In[25]:


final_data.show(5)


# In[26]:


df = final_data.select(['features', 'claim_flag'])
df =  df.withColumnRenamed('Claim_flag', "label")
df.show(5)


# In[27]:


train, test = df.randomSplit([0.7, 0.3], seed = 123)


# ## Models
# 
# \
# 
# 

# ### Logistic Regression

# In[28]:


from pyspark.ml.classification import LogisticRegression


# In[29]:


log_reg = LogisticRegression(maxIter=10).fit(train)


# In[30]:


lr_summary = log_reg.summary


# In[31]:


# Overall accuracy of model
lr_summary.accuracy


# In[32]:


# Area under ROC
lr_summary.areaUnderROC


# In[33]:


# Precision of both classes
print(lr_summary.precisionByLabel)


# In[34]:


# Recall of both classes
print(lr_summary.recallByLabel)


# In[35]:


# Predictions
predictions = log_reg.transform(test)


# In[36]:


predictions.select('label', 'prediction').show(10)


# In[37]:


from sklearn.metrics import confusion_matrix
y_true = predictions.select("label")
y_true = y_true.toPandas()

y_pred = predictions.select("prediction")
y_pred = y_pred.toPandas()
class_names = [1, 0]
cnf_matrix = confusion_matrix(y_true, y_pred,labels=class_names)
cnf_matrix


# ### Random Forest Classifier

# In[38]:


from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier()
rfModel = rf.fit(train)
rfPreds = rfModel.transform(test)


# In[39]:


rfPreds.select('label', 'rawPrediction', 'prediction', 'probability').show(10)


# In[40]:


rfModel_summary = rfModel.summary
rfModel_summary.accuracy


# ### Gradient-Boosted Tree Classifier

# In[41]:


from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(maxIter=10)
gbtModel = gbt.fit(train)
gbtPreds = gbtModel.transform(test)


# In[42]:


gbtPreds.select('label', 'rawPrediction', 'prediction', 'probability').show(20)

