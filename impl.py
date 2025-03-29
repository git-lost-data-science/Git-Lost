from csv import reader
from pprint import pprint 
# 1
# DATA MODEL CLASSES

class IdentifiableEntity(object):
    def __init__(self, id:str):
        pass
    
    def getId(self):
        return self.id
    
class Category(IdentifiableEntity):
    def __init__(self, quartile: str):
        pass
    def getQuartile(): # str or None
        pass

class Journal(IdentifiableEntity):
    pass # building the Journal class 

    def getTitle(): # returns a string 
        pass
    def getLanguage(): # returns a list of strings
        pass
    def getPublisher(): # string or None
        pass 
    def hasDOAJSeal(): # bool 
        pass
    def getLicence(): # str
        pass
    def hasAPC(): # bool
        pass
    def getCategories(): #hasCategory 0...*, related to the class Category, returns a list[Category]
        pass
    def getAreas(): #hasArea 0...*, related to the class Area, returns a list[Area]
        pass

class Area(IdentifiableEntity):
    pass

# 2
# HANDLER (note: we can also add other methods, but the contructors do not take any parameter in input)

class Handler(object): 
    pass
    def getDbPathOrUrl(): # str
        pass
    def setDbPathOrUrl(self, pathOrUrl:str):# bool: check if the path is valid
        pass

# UPLOAD HANDLER 

class UploadHandler(Handler):
    def __init__(self):
        pass
    def pushDataToDb(self, path:str): # returns a bool 
        pass

class JournalUploadHandler(UploadHandler): # handles CSV files
    pass

class CategoryUploadHandler(UploadHandler): # handles JSON files
    pass

# QUERY HANDLER 
class QueryHandler(Handler):
    pass
    def getById(id:str): # returns a DataFrame
        pass

# 3

class CategoryQueryHandler(QueryHandler):
    pass
    def getById(id:str): # DataFrame
        pass 

class JournalQueryHandler(): # all the methods return a DatafRame
    pass
    def getAllJournals(): 
        pass
    def getJournalsWithTitle(self, partialTitle:str): 
        pass
    def getJournalsPublishedBy(self, partialName: str):
        pass
    def getJournalsWothLicense(self, licenses:set[str]):
        pass
    def JournalsWithAPC():
        pass
    def JournalsWithDOAJSeal():
        pass

# 4

# FULL QUERY ENGINE
class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]): 
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str]): 
        pass
    def getDiamondJournalsAreasAmdCAtegoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]):
        pass

class BasicQueryEngine(object):
    pass
    def cleanJournalHandlers(): # bool 
        pass
    def cleanCategoryHanders(): #bool 
        pass
    # etc. 
    # testing 
