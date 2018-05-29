class Utils:
    def filter_list(self,list):
        newList=[]
        for i in list:
            if i not in newList:
                newList.append(i)
        return newList 
    def lang_exists(lang,languages):
        for i in range(0,len(languages)):
            if lang==languages[i]["name"]:
                return i
        return -1

    def url_exists(repourl,url_list):
        return repourl in url_list
