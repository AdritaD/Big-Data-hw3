

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
import math

config = SparkConf().setAppName('ratings').setMaster('local')

sparkconfig = SparkContext.getOrCreate(conf=config)

Ratings_datFile = sparkconfig.textFile("ratings.dat")

Ratings_datFile = Ratings_datFile.map(lambda x: x.split("::"))

Ratings_datFile = Ratings_datFile.map(lambda tokens: (tokens[0], tokens[1], tokens[2]))

Trainingdata_RDD, Testdata_RDD = Ratings_datFile.randomSplit([6, 4], seed=0)

TestdatForPrediction = Testdata_RDD.map(lambda x: (x[0], x[1]))

seeds = 5
numberofIterations = 20
ranks_arr = [4, 8, 12, 16]
errors_arr = [0, 0, 0, 0]
err_Val = 0
tolerance = 0.02
reg_fac = 0.1

min_ErrorVal = float('inf')
best_rankObtained = -1
best_IterObtained = -1

for eachrank in ranks_arr:
    model = ALS.train(Trainingdata_RDD, eachrank, seed=seeds, iterations=numberofIterations, lambda_=reg_fac)
    predictions = model.predictAll(TestdatForPrediction).map(lambda r: (((r[0], r[1]), int(round(r[2])))))
    rates_preds = Testdata_RDD.map(lambda x: (((int(x[0]), int(x[1])), float(x[2])))).join(predictions)

    combined_result=rates_preds.collect()
    sum_ = 0
    for i in range(len(combined_result)):
        if combined_result[i][1][0] == combined_result[i][1][1]:
            sum_ += 1
    print("ALS Accuracy for rank  %s is %s: "%(eachrank, sum_ / float(len(combined_result))))


    error = math.sqrt(rates_preds.map(lambda x: (x[1][0] - x[1][1]) ** 2).mean())
    errors_arr[err_Val] = error
    err_Val += 1

    print("For rank %s the RMSE is %s" % (eachrank, error))

    if (error < min_ErrorVal):
        min_ErrorVal = error
        best_rankObtained = eachrank
print("The best model was trained with rank ", best_rankObtained)