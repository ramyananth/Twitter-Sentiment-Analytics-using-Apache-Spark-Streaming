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
    posCount = []
    negCount = []
    
    for temp in counts:
        for word in temp:
            if word[0] == "positive":
                posCount.append(word[1])
            else:
                negCount.append(word[1])
                
    plt.axis([-1, len(posCount), 0 , max(max(posCount), max(negCount))+15000])
    pos, = plt.plot(posCount, 'go--', markersize =12)
    neg, = plt.plot(negCount, 'bo--', markersize =12)
    plt.xlabel('Time Step')
    plt.ylabel('Word Count')
    plt.legend((pos,neg),('Positive', 'Negative'), loc=2)
    plt.savefig('img.png')


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    file = open(filename, 'r')
    words =[]
    for temp in file:
        words.append(temp.split("\n")[0])
    return set(words)

def updateFunction(newSum, oldSum):
    if oldSum is None:
        oldSum = 0
    return sum(newSum, oldSum)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    tweets = tweets.flatMap(lambda x:x.split(" ")).filter(lambda word:(word in pwords) or (word in nwords)).map(lambda word: ('positive', 1) if (word in pwords) else ('negative', 1)).reduceByKey(lambda x, y : x+y)

    updatedWords = tweets.updateStateByKey(updateFunction)
    updatedWords.pprint()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).   
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    tweets.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()