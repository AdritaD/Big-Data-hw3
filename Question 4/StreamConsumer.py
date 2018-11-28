from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json




def main():
    '''
    Consumer consumes tweets from producer
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer('twitter')
    sid = SentimentIntensityAnalyzer()
    i=0
    positiveCounter = 0
    negativeCounter = 0
    neutralCounter = 0
    compoundCounter =0

    for msg in consumer:
        output=json.loads(msg.value)
        text = output["text"]
        # print("text:", text)
        ss=sid.polarity_scores(text)
        # print('Compound {0} Negative {1} Neutral {2} Positive {3} '.format(ss['compound'], ss['neg'], ss['neu'],ss['pos']))

        maxValueKey =max(ss, key=ss.get)
        maxValue = ss[maxValueKey]
        # print("maxValueKey:",maxValueKey)

        if(maxValueKey =='compound'):
            compoundCounter += 1

        elif (maxValueKey == 'neg'):
            negativeCounter += 1
            if (negativeCounter<=5):
                print("negative text:",text)
                print("Value",maxValue)
                print("negativeCounter:",negativeCounter)
                print('\n')

        elif (maxValueKey == 'neu'):
            neutralCounter += 1
            if (neutralCounter<=5):
                print("neutral text:",text)
                print("Value",maxValue)
                print("neutralCounter:", neutralCounter)
                print('\n')

        elif (maxValueKey == 'pos'):
            positiveCounter += 1
            if (positiveCounter<=5):
                print("positive text:", text)
                print("Value", maxValue)
                print("positiveCounter:", positiveCounter)
                print('\n')



        if(negativeCounter >=5 and neutralCounter >=5 and positiveCounter>=5):
            break





if __name__ == "__main__":
    main()