from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
import os

def initialize_spark():
    appName = "SparkwithMlib"
    master = "local"
    spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
    return spark

def read_data(spark):
    query = "SELECT * FROM project1db.carinsuranceclaims"
    data = spark.sql(query)
    return data

def clean_data(data):
    def strip_special_characters(x):
        if isinstance(x, str):
            return x.replace("$", "").replace(",", "")
        else:
            return x
    
    udfdollarsign = udf(strip_special_characters, StringType())
    
    columns_to_transform = ["home_val", "income", "bluebook", "oldclaim", "clm_amt"]
    for i in columns_to_transform:
        data = data.withColumn(i, udfdollarsign(data[i]))

    data = data.withColumn("age", data["age"].cast(FloatType()))
    data = data.withColumn("yoj", data["yoj"].cast(FloatType()))
    data = data.withColumn("income", data["income"].cast(FloatType()))
    data = data.withColumn("home_val", data["home_val"].cast(FloatType()))
    data = data.withColumn("bluebook", data["bluebook"].cast(FloatType()))
    data = data.withColumn("oldclaim", data["oldclaim"].cast(FloatType()))
    data = data.withColumn("clm_amt", data["clm_amt"].cast(FloatType()))
    data = data.withColumn("car_age", data["car_age"].cast(FloatType()))

    data = data.withColumn('mstatus', F.when(F.col('mstatus') == 'Married', 'Yes').
                            when(F.col('mstatus') == 'Single', 'No').
                            otherwise(F.col('mstatus')))
    
    data = data.withColumn('gender', F.when(F.col('gender') == 'Female', 'F')
                            .when(F.col('gender') == 'Male', 'M')
                            .otherwise(F.col('gender')))
    
    data = data.withColumn('red_car', F.when(F.col('red_car') == 'No', 'no')
                            .when(F.col('red_car') == 'Yes', 'yes')
                            .otherwise(F.col('red_car')))
    
    return data

def preprocess_data(data):
    indexer = StringIndexer(inputCols=catcols, outputCols=[col + "_indexed" for col in catcols])
    data = indexer.fit(data).transform(data)
    
    OHE = OneHotEncoder(inputCols=['parent1_indexed', 'mstatus_indexed',
                                   'gender_indexed', 'education_indexed',
                                   'car_use_indexed', 'car_type_indexed',
                                   'red_car_indexed',
                                   'revoked_indexed'], outputCols=['parent1_OHE', 'mstatus_OHE',
                                   'gender_OHE', 'education_OHE',
                                   'car_use_OHE', 'car_type_OHE',
                                   'red_car_OHE',
                                   'revoked_OHE'])
    data = OHE.fit(data).transform(data)
    
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
    
    df = final_data.select(['features', 'claim_flag'])
    df =  df.withColumnRenamed('Claim_flag', "label")
    
    return df

def train_test_split(df):
    train, test = df.randomSplit([0.7, 0.3], seed=123)
    return train, test

def evaluate_model(predictions):
    evaluator = BinaryClassificationEvaluator()
    accuracy = evaluator.evaluate(predictions)
    return accuracy

def main():
    spark = initialize_spark()
    data = read_data(spark)
    data = clean_data(data)
    df = preprocess_data(data)
    train, test = train_test_split(df)

    # Logistic Regression
    log_reg = LogisticRegression(maxIter=10).fit(train)
    predictions_lr = log_reg.transform(test)
    accuracy_lr = evaluate_model(predictions_lr)

    # Random Forest Classifier
    rf = RandomForestClassifier()
    rfModel = rf.fit(train)
    predictions_rf = rfModel.transform(test)
    accuracy_rf = evaluate_model(predictions_rf)

    # Gradient-Boosted Tree Classifier
    gbt = GBTClassifier(maxIter=10)
    gbtModel = gbt.fit(train)
    predictions_gbt = gbtModel.transform(test)
    accuracy_gbt = evaluate_model(predictions_gbt)

    print("Logistic Regression Accuracy:", accuracy_lr)
    print("Random Forest Classifier Accuracy:", accuracy_rf)
    print("Gradient-Boosted Tree Classifier Accuracy:", accuracy_gbt)

if __name__ == "__main__":
    main()
