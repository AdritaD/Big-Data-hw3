from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("q2-nb")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# obtaining the data file
data = sc.textFile("glass.data")

# Splitting each line of the data by ","
data = data.map(lambda x: str(x).split(","))

# Splitting the data into training and test set
train_data, test_data = data.randomSplit([0.6, 0.4])

# Changing the data into LabelledPoint format
train_data = train_data.map(lambda row: LabeledPoint(row[-1], row[:-1]))

# Converting RDD to lists
train_data = train_data.collect()
test_data = test_data.collect()

# Removing labels from each test data row and saving it in test_labels

test_labels = []
for i in range(len(test_data)):
    test_labels.append(float(test_data[i][10]))
    del test_data[i][10]

# Converting test_data into RDD
test_data = sc.parallelize(test_data)

# Creating a model using the training data
model = NaiveBayes.train(sc.parallelize(train_data))

# Predicting test_data
prediction = model.predict(test_data)

# Converting prediction from rdd to a list
prediction = prediction.collect();

# Calculating the accuracy
sum_ = 0

for i in range(len(test_labels)):
    if prediction[i] == test_labels[i]:
        sum_ += 1

print("Accuracy of Naive Bayes:", sum_/float(len(test_labels)))