from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from firebase import firebase
import requests

from config import Config
config=Config()

def process(data,firebase):
    github_url=config.github_url
    tmp=[]
    for d in data:
        id=d["id"]
        type=d["type"]
        user,reponame=d["repo"]["name"].split("/")
        url=d["repo"]["url"]
        repourl="%s%s"%(github_url,d["repo"]["name"])
        print("ID {} -- TYPE {} -- NAME {} -- URL {} ".format(id,type,reponame,url))
        repo=requests.get(url).json()
        if "language" in repo:
            print("LANGAUAGE {} -- WATCHERS {} -- FORKS {} -- ISSUES {}"\
            .format(repo["language"],repo["watchers_count"],repo["forks_count"],\
            repo["open_issues"]))
            tmp+=[{"repo_url":repourl,"language":repo["language"],"rate":repo["watchers_count"]+repo["forks_count"],}]
    sortedList = sorted(tmp, key=lambda k: k["rate"],reverse=True)
    l=firebase.put('/batch','top_repos',sortedList)
    return sortedList


if __name__ == "__main__":
    database_url=config.database_url
    print("**** BATCH PROCESSING ****")
    firebase = firebase.FirebaseApplication(database_url)
    print("--- CONNECTED TO FIREBASE ---")
    conf = SparkConf().setAppName("SparkOnDemandBatch").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    session=SQLContext(sc)
    path=config.hdfs_storage_path
    lines = sc.textFile(path)
    responses = dataFrameReader\
    .option("chartset","UTF-8")\
    .option("multiline","true")\
    .json(lines)
    data=responses.collect()
    sortedList=process(data,firebase)
    for s in sortedList:
        print(s)
