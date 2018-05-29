from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from firebase import firebase
import requests

#My Libraries
from utils import Utils
from config import Config

config=Config()


def process(dfr,firebase,rdd):
    access_token=config.github_oauth_token
    total_events(dfr,firebase,rdd)
    top_technologies(dfr,firebase,rdd,access_token)

def total_events(dfr,firebase,rdd):
    print("**** TOTALS PROCESS ****")
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








def top_technologies(dfr,firebase,rdd,access_token):
    github_url=config.github_url
    database_url=config.database_url
    frameworks=[{"name":"Angular","count":0,"url_list":["none"]},\
           {"name":"Vue","count":0,"url_list":["none"]},\
           {"name":"React","count":0,"url_list":["none"]},\
           {"name":"Android","count":0,"url_list":["none"]},\
           {"name":"Native","count":0,"url_list":["none"]},\
           {"name":"Electron","count":0,"url_list":["none"]},\
           {"name":"Spring","count":0,"url_list":["none"]},\
           {"name":"Jsf","count":0,"url_list":["none"]},\
           {"name":"Laravel","count":0,"url_list":["none"]},\
           {"name":"Symphony","count":0,"url_list":["none"]},\
           {"name":"Ethereum","count":0,"url_list":["none"]},\
           {"name":"Blockchain","count":0,"url_list":["none"]},\
           {"name":"Jquery","count":0,"url_list":["none"]},\
           {"name":"Ionic","count":0,"url_list":["none"]}]





    langs=[{"name":"Python","count":0,"url_list":["none"]},\
           {"name":"HTML","count":0,"url_list":["none"]},\
           {"name":"C","count":0,"url_list":["none"]},\
           {"name":"C++","count":0,"url_list":["none"]},\
           {"name":"Java","count":0,"url_list":["none"]},\
           {"name":"JavaScript","count":0,"url_list":["none"]},\
           {"name":"Node","count":0,"url_list":["none"]},\
           {"name":"Scala","count":0,"url_list":["none"]},\
           {"name":"PHP","count":0,"url_list":["none"]},\
           {"name":"TypeScript","count":0,"url_list":["none"]},\
           {"name":"Go","count":0,"url_list":["none"]},\
           {"name":"Ruby","count":0,"url_list":["none"]},\
           {"name":"Haskell","count":0,"url_list":["none"]},\
           {"name":"Shell","count":0,"url_list":["none"]}]


    jsonDataEvents=dfr\
    .option("charset","UTF-8")\
    .option("multilne","true")\
    .json(rdd)
    data=jsonDataEvents\
        .filter(jsonDataEvents["type"]=="CreateEvent")\
        .collect()
    totalEvents=len(data)
    print("\033[92mGOT EVENTS\033[0m")
    print("\033[94mTOTAL EVENTS: %d\033[0m"%totalEvents)
    for d in data:
        id=d["id"]
        type=d["type"]
        user,reponame=d["repo"]["name"].split("/")
        url=d["repo"]["url"]
        repourl="%s%s"%(github_url,d["repo"]["name"])
        print("ID {} -- TYPE {} -- NAME {} ".format(id,type,reponame))
        ok=0
        for f in frameworks:
            if f["name"].lower() in reponame.lower():
                f["count"]+=1
                f["url_list"]+=[repourl]
                ok=1
                break
        if ok==0:
            repo=requests.get(url,headers={'Authorization': 'token %s'%access_token}).json()
            if "language" in repo:
                print("LANGAUAGE {} -- WATCHERS {} -- FORKS {} -- ISSUES {}  "\
                .format(repo["language"],repo["watchers_count"],repo["forks_count"],repo["open_issues"]))
                index=Utils().lang_exists(repo["language"],langs)
                if index!=-1:
                    langs[index]["count"]+=1
                    if Utils().url_exists(repourl,langs[index]["url_list"])==False:
                        langs[index]["url_list"]+=[repourl]
            else:
                print("API CLOSED!!!")


## FRAMEWORKS HERE
    print("\033[93mFRAMEWORKS {}\033[0m".format(frameworks))
    oldFw=firebase.get('/streaming/frameworks',None)
    print("OLD FRAMEWORKS {}".format(oldFw))
    if oldFw!=None:
        for i in range(0,len(frameworks)):
           frameworks[i]["count"]=frameworks[i]["count"]+oldFw[i]["count"]
           tmp=frameworks[i]["url_list"]+oldFw[i]["url_list"]
           url_list=Utils().filter_list(tmp)
           frameworks[i]["url_list"]=tmp
    newFw=firebase.put('/streaming/','frameworks',frameworks)
    print("NEW FRAMEWORKS {} ".format(newFw))

    print("-----------------------------------------------------------------")
    print("\033[91mLANGUAGES {}\033[0m".format(langs))

## LANGUAGES HERE
    oldLangs=firebase.get('/streaming/languages',None)
    print("OLD LANGUAGES {}".format(oldLangs))
    if oldLangs!=None:
        for i in range(0,len(langs)):
           langs[i]["count"]+=oldLangs[i]["count"]
           tmp=langs[i]["url_list"]+oldLangs[i]["url_list"]
           url_list=Utils().filter_list(tmp)
           langs[i]["url_list"]=tmp
    newLangs=firebase.put('/streaming/','languages',langs)
    print("NEW LANGUAGES {} ".format(newLangs))

if __name__ == '__main__':
    database_url=config.database_url
    print("------ RUNNING SPARK STREAMING APP ------")
    firebase = firebase.FirebaseApplication(database_url)
    print("--- CONNECTED TO FIREBASE ---")
    conf = SparkConf().setAppName('KafkaSparkStreamingConsumer').setMaster('local[*]')
    sc=SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 2) # 2 second window
    zkQuorm = "localhost:2181"
    topic = "ProjectTopic"
    messages = KafkaUtils.createStream(ssc, \
                                  zkQuorm, \
                                  'bigdata-project-group',{topic:1})


    lines = messages.map(lambda x: x[1])
    dataFrameReader = sqlContext.read
    lines.foreachRDD(lambda rdd: process(dataFrameReader,firebase,rdd) if (rdd.count()) else False)
    ssc.start()
    ssc.awaitTermination()
