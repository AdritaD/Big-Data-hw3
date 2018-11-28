from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("Problem2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# obtaining the data file
data = sc.textFile("glass.data")

# Splitting each line of the data by ","
data = data.map(lambda x: str(x).split(","))

# Splitting the data into training and test set by 60% and 40%
train_data, test_data = data.randomSplit([0.6, 0.4])

# Converting the class labels from range 1-7 to 0-6 - Decision Tree Model needs the labels to  be <numClasses
train_data = train_data.collect()  # converting RDD to list
for i in range(len(train_data)):
    # converting the class label column into int, reducing it by 1 and converting it back to string
    train_data[i][10] = str(int(train_data[i][10])-1)

# converting train_data from list , back to RDD
train_data = sc.parallelize(train_data)

# Changing the data into LabelledPoint format - label for each row is the class label found at row[10] or row[-1]
train_data = train_data.map(lambda row: LabeledPoint(row[-1], row[:-1]))

# Converting RDD to lists
test_data = test_data.collect()

# Removing labels from each test data row and saving it in test_labels
test_labels = []
for i in range(len(test_data)):
    test_labels.append(float(test_data[i][10])-1)
    del test_data[i][10]

# Converting test_data into RDD
test_data = sc.parallelize(test_data)

# Creating a model using the training data
model = DecisionTree.trainClassifier(data=train_data, numClasses=7, categoricalFeaturesInfo={})

# Predicting test_data
prediction = model.predict(test_data)

# Converting prediction from rdd to a list
prediction = prediction.collect();

# Calculating the accuracy
sum_ = 0

for i in range(len(test_labels)):
    if prediction[i] == test_labels[i]:
        sum_ += 1

print("Accuracy of Decision Tree:", sum_/float(len(test_labels)))