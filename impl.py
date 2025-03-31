from csv import reader
from pprint import pprint 
from sqlite3 import connect 
from json import load
from pandas import DataFrame, Series

from pyOptional import Optional

class IdentifiableEntity():
    def __init__(self, id:list|str): # one or more strings. Just covering any case 
        if not isinstance(id, list) or not all(isinstance(i, str) for i in id):
            raise TypeError(f"Expected a list of str or a str, got {type(id).__name__}")
        self.id = id 

    def getIds(self):
        return list(self.id)
    
class Category(IdentifiableEntity):
    def __init__(self, id, quartile: Optional[str]): # 1 str or None 
        super().__init__(id) #inherits from its superclass 
        if quartile is not None and not isinstance(quartile, str):
            raise TypeError(f"Expected a NoneType or str, got {type(quartile).__name__}")
        self.quartile = quartile 
        
    def getQuartile(self): 
        return self.quartile 

class Area(IdentifiableEntity): # 0 or more. Nothing to add, inherits the methods of the super()
    pass 

class Journal(IdentifiableEntity):
    def __init__(self, id, title: str, languages: str|list, publisher: Optional[str], 
                 seal: bool, licence: str, apc: bool, hasCategory: Optional[list[Category]], 
                 hasArea: Optional[list[Area]]):
        super().__init__(id)

        if not isinstance(title, str) or not title:
            raise TypeError(f"Expected a non-empty str, got {type(title).__name__}")
        
        if not isinstance(languages, list) or not all(isinstance(lang, str) for lang in languages) or not languages:
            raise TypeError(f"Expected a non-empty str or list, got {type(languages).__name__}")
        
        if not(isinstance(publisher, str) or isinstance(publisher, None)):
            raise TypeError(f"Expected a str or a NoneType, got {type(publisher).__name__}")
        
        if not isinstance(seal, bool):
            TypeError(f"Expected a boolean, got {type(seal).__name__}")
        
        if not isinstance(licence, str) or not licence:
            raise TypeError(f"Expected a non-empty str, got {type(licence).__name__}")
        
        if not isinstance(apc, bool):
            raise TypeError(f"Expected a boolean, got {type(apc).__name__}")
        
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
        self.hasCategory = hasCategory   # ! List of Category objects, CHECK !
        self.hasArea =  hasArea # ! List of Area objects, CHECK 

#      def addCategory(self, category): # ! CHECK if it is necessary!
#         if not isinstance(category, Category):
#             raise ValueError("category must be an instance of Category.")
#         self.categories.append(category)

#     def addArea(self, area): # same here
#         if not isinstance(area, Area):
#             raise ValueError("area must be an instance of Area.")
#         self.areas.append(area) """

    def getTitle(self):
        return self.title

    def getLanguage(self):
        return list(self.languages)

    def getPublisher(self):
        return self.publisher

    def hasDOAJSeal(self):
        return self.seal

    def getLicence(self):
        return self.licence

    def hasAPC(self):
        return self.apc

    def getCategories(self):
        return list(self.hasCategory)

    def getAreas(self):
        return list(self.hasArea)



# HANDLERS (note: we can also add other methods, but the contructors do not take any parameter in input)

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
