
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils

import sys
reload(sys)
sys.setdefaultencoding('utf8')


def process(dfr,rdd):
    print("\033[92mGOT EVENTS\033[0m")
    events=rdd.collect()
    totalEvents=len(events)
    print("\033[94mTOTAL EVENTS: %d\033[0m"%totalEvents)
    jsonDataEvents=dfr\
    .option("charset","UTF-8")\
    .option("multilne","true")\
    .json(rdd)
    createEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="CreateEvent").count()
    pushEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="PushEvent").count()
    pullRequestsEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="PullRequestEvent").count()
    deleteEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="DeleteEvent").count()
    issueEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="IssuesEvent").count()
    watchEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="WatchEvent").count()
    forkEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="ForkEvent").count()
    memberEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="MemberEvent").count()
    print("Number of create events %d"%createEvents)
    print("Number of push events %d"%pushEvents)
    print("Number of pull requests %d"%pullRequestsEvents)
    print("Number of delete events %d"%deleteEvents)
    print("Number of issue events %d"%issueEvents)
    print("Number of watch events %d"%watchEvents)
    print("Number of fork evetns %d"%forkEvents)
    print("Number of new members %d"%memberEvents)


#    for i in events:
#        jsonData = dfr\
#        .option("charset","UTF-8")\
#        .option("multiline","true")\
#        .json(sc.parallelize([i]))
#        print("--- ID - ACTOR - TYPE - CREATED_AT ---")
#        data=jsonData.first()
#        print("--- {} - {} - {} - {} ---".format(data["id"],data["actor"],data["type"],\
#        data["created_at"]))

if __name__ == '__main__':
    print("Running SPARK STREAMING APP")
    conf = SparkConf().setAppName('KafkaSparkConsumer').setMaster('local[2]')
    sc=SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 2) # 2 second window
    zkQuorm = "localhost:2181"
    topic = "ProjectTopic"
    messages = KafkaUtils.createStream(ssc, \
                                  zkQuorm, \
                                  'bigdata-project-group',{topic:1})


    lines = messages.map(lambda x: x[1])
    #lines.pprint()
    dataFrameReader = sqlContext.read
    lines.foreachRDD(lambda rdd: process(dataFrameReader,rdd) if (rdd.count()) else False)
    ssc.start()
    ssc.awaitTermination()
