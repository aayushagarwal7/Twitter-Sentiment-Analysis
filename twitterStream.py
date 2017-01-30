from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
 
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    
    positiveCount = []
    negativeCount = []
    
    for count in counts:
        for word in count:
            if word[0] == "positive":
                positiveCount.append(word[1])
            else:
                negativeCount.append(word[1])

    plt.axis([-1, len(positiveCount), 0, max(max(positiveCount),max(negativeCount))+50])
    pos, = plt.plot(positiveCount, 'b-', marker = 'o', markersize = 8)
    neg, = plt.plot(negativeCount, 'g-', marker = 'o', markersize = 8)
    plt.xticks(np.arange(0, len(positiveCount), 1))
    plt.legend((pos,neg),('positive','negative'),loc=2)
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.show()

def load_wordlist(filename):
    """
    This function should return a list or set of words from the given filename.
    """
    wordData = [line.strip() for line in open(filename, 'r')]
    return wordData

def updateFunction(newValue, runningCount):
    if runningCount is None:
        runningCount = 0 
    return sum(newValue, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    wordsFromTweets = tweets.flatMap(lambda line:line.split(" "))
    wordsPosNeg = wordsFromTweets.filter(lambda word:(word in pwords) or (word in nwords)).map(lambda word:('positive', 1) if (word in pwords) else ('negative', 1))
    wordCounts = wordsPosNeg.reduceByKey(lambda x, y: x + y)
    runningCounts = wordsPosNeg.updateStateByKey(updateFunction)
    runningCounts.pprint()
    
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

