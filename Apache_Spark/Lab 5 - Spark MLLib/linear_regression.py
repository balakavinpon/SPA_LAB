from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

sc= SparkContext()
sqlContext = SQLContext(sc)

house_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('boston.csv')

print("**************************")
print(house_df)
print(house_df.printSchema())
print(house_df.describe().toPandas().transpose())
print("**************************")

#"crim","zn","indus","chas","nox","rm","age","dis","rad","tax","ptratio","black","lstat","medv"
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ["crim","zn","indus","chas","nox","rm","age","dis","rad","tax","ptratio","black","lstat"], outputCol = 'features')

vhouse_df = vectorAssembler.transform(house_df)
vhouse_df = vhouse_df.select(['features', 'medv'])
print("**************************")
vhouse_df.show(3)
print("**************************")

splits = vhouse_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='medv', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("**********Coefficients: " + str(lr_model.coefficients))
print("**********Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("**********RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("**********r2: %f" % trainingSummary.r2)

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","medv","features").show(5)

from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="medv",metricName="r2")
print("**********R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

test_result = lr_model.evaluate(test_df)
print("**********Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

predictions = lr_model.transform(test_df)
predictions.select("prediction","medv","features").show()