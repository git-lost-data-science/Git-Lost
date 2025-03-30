from csv import reader
from pprint import pprint 
from sqlite3 import connect 
from json import load
from pandas import DataFrame, Series

# DATA MODEL CLASSES

class IdentifiableEntity():
    def __init__(self, id:list): # one or more strings
        if not isinstance(id, list) or not all(isinstance(i, str) for i in id):
            raise ValueError('IdentifiableEntity.id must be one or more strings')
        self.id = id
    
    def getId(self):
        return self.id
    
class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: str| None): # 1 str or None 
        super().__init__(id) #inherits from its superclass :)
        if quartile is not None and not isinstance(quartile, str):
            raise ValueError("Nope! Quartile must be a string or None!")
        self.quartile = quartile 
        
    def getQuartile(self): 
        return self.quartile 

class Journal(IdentifiableEntity):
    def __init__(self, id:list, title: str, languages: str|list, publisher: str|None, seal: bool, licence: str, pac: bool, hasCategory: list[Category] = None, hasArea: [Area] = None):
        super().__init__(id)
        if not isinstance(title, str) or not title:
            raise ValueError("Title must be a non-empty string.")
        
        if isinstance(languages, str):
            languages = [languages]
        elif not isinstance(languages, list) or not all(isinstance(lang, str) for lang in languages) or not languages:
            raise ValueError("Languages must be a non-empty list of strings or a single string.")
        
        if publisher is not None and not isinstance(publisher, str):
            raise ValueError("Publisher must be a string or None.")
        
        if not isinstance(seal, bool):
            raise ValueError("Seal must be a boolean value.")
        
        if not isinstance(licence, str) or not licence:
            raise ValueError("Licence must be a non-empty string.")
        
        if not isinstance(pac, bool):
            raise ValueError("PAC must be a boolean value.")
        
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.pac = pac
        self.categories = hasCategory if hasCategory else []  # List of Category objects, CHECK !
        self.areas =  hasArea if hasArea else [] # List of Area objects, CHECK !

    def addCategory(self, category): # idk it it is necessary actually
        if not isinstance(category, Category):
            raise ValueError("category must be an instance of Category.")
        self.categories.append(category)

    def addArea(self, area): # same here
        if not isinstance(area, Area):
            raise ValueError("area must be an instance of Area.")
        self.areas.append(area)

    def getTitle(self):
        return self.title

    def getLanguage(self):
        return self.languages

    def getPublisher(self):
        return self.publisher

    def hasDOAJSeal(self):
        return self.seal

    def getLicence(self):
        return self.licence

    def hasAPC(self):
        return self.pac

    def getCategories(self):
        return self.categories

    def getAreas(self):
        return self.areas

class Area(IdentifiableEntity): # 0 or more. Nothing to add, inherits the methods of the super()
    pass

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
