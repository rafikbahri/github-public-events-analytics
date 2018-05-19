
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from firebase import firebase

def process(dfr,firebase,rdd):
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
    releaseEvents=jsonDataEvents\
    .filter(jsonDataEvents["type"]=="ReleaseEvent").count()
    consideredEvents=createEvents+pushEvents+pullRequestsEvents+deleteEvents+issueEvents+watchEvents+forkEvents+memberEvents+releaseEvents
    otherEvents=totalEvents-consideredEvents
    print("Number of create events %d"%createEvents)
    print("Number of push events %d"%pushEvents)
    print("Number of pull requests %d"%pullRequestsEvents)
    print("Number of delete events %d"%deleteEvents)
    print("Number of issue events %d"%issueEvents)
    print("Number of watch events %d"%watchEvents)
    print("Number of fork evetns %d"%forkEvents)
    print("Number of new members %d"%memberEvents)
    print("Number of release events %d"%releaseEvents)
    print("Other events %d"%otherEvents)
    #Get the values from firebase
    oldTotals=firebase.get('/totals',None)
    print(oldTotals)
    #Update the totals
    updatedTotals=firebase.put('/','totals',\
    {'total_events':oldTotals['total_events']+totalEvents,\
     'release_events':oldTotals['release_events']+releaseEvents,\
     'other_events':oldTotals['other_events']+otherEvents,\
     'member_events':oldTotals['member_events']+memberEvents,\
     'create_events':oldTotals['create_events']+createEvents,\
     'pull_request_events':oldTotals['pull_request_events']+pullRequestsEvents,\
     'issues_events':oldTotals['issues_events']+issueEvents,\
     'push_events':oldTotals['push_events']+pushEvents,\
     'fork_events':oldTotals['fork_events']+forkEvents,\
     'delete_events':oldTotals['delete_events']+deleteEvents,\
     'watch_events':oldTotals['watch_events']+watchEvents})

    print("UPDATED ",updatedTotals)

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
    print("------ RUNNING SPARK STREAMING APP ------")
    firebase = firebase.FirebaseApplication('https://github-events-bigdata.firebaseio.com/')
    print("--- CONNECTED TO FIREBASE ---")
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
    lines.foreachRDD(lambda rdd: process(dataFrameReader,firebase,rdd) if (rdd.count()) else False)
    ssc.start()
    ssc.awaitTermination()
