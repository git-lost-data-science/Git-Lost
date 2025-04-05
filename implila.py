from csv import reader
from pprint import pprint 
from sqlite3 import connect 
from json import load
from pandas import DataFrame, Series, pd
# ? import uuid

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
    def __init__(self):
        self.dbPathOrUrl = ""

    # @property 
    def getDbPathOrUrl(self): 
        return self.dbPathOrUrl 
     
    # @getDbPathOrUrl.setter
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:  # setter
        if not pathOrUrl.strip():  # controlla se è vuota o solo spazi
            return False
        if pathOrUrl.endswith(".db"):
            self.dbPathOrUrl = pathOrUrl
            return True
        elif pathOrUrl.startswith("http://") or pathOrUrl.startswith("https://"):
            self.dbPathOrUrl = pathOrUrl
            return True
        return False

# UPLOAD HANDLER 

class UploadHandler(Handler):
    def __init__(self):
        pass
    def pushDataToDb(self, path:str) -> bool: # ! check!!!
        try:
            conn = connect(db_path)
            cursor = conn.cursor()
            
            # Create tables if they don't exist
            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Journal (
                    internal_id INTEGER PRIMARY KEY,
                    identifiers TEXT
                )
            ''')

            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Category (
                    internal_id INTEGER PRIMARY KEY,
                    journal_internal_id INTEGER,
                    category_id TEXT,
                    quartile TEXT,
                    FOREIGN KEY (journal_internal_id) REFERENCES Journal(internal_id)
                )
            ''')

            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Area (
                    internal_id INTEGER PRIMARY KEY,
                    journal_internal_id INTEGER,
                    area TEXT,
                    FOREIGN KEY (journal_internal_id) REFERENCES Journal(internal_id)
                )
            ''')

            # Insert data into the tables
            journal_df.to_sql('Journal', conn, if_exists='append', index=False)
            category_df.to_sql('Category', conn, if_exists='append', index=False)
            area_df.to_sql('Area', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close() # ? maybe with 'with' it is not needed? check
            return True
        except Exception as e:
            print(f"Error uploading data: {e}")
            return False
        

class JournalUploadHandler(UploadHandler): # handles CSV files
    pass

class CategoryUploadHandler(UploadHandler): # handles JSON files
    def __init__(self):
        self.internal_id_counter = 0  # Contatore per gli internal_id numerici

    def generate_internal_id(self):
        internal_id = self.internal_id_counter
        self.internal_id_counter += 1 # starts from 1
        return internal_id
    
    def pushDataToDb(self, json_path: str, db_path: str) -> bool: # ! idk the relationship wiht the super
        # Load JSON data
        with open(json_path, 'r') as file:
            data = load(file)

        # Create internal IDs and build DataFrames
        journals = []
        categories = []
        areas = []
        
        # Internal IDs
        journal_id_map = {}  # to map old identifiers to internal IDs that I created
        category_id_map = {}  # same for category
        area_id_map = {}  # same for area
        
        for journal in data:
            # Generate a unique internal ID for the journal
            journal_internal_id = self.generate_internal_id()
            journal_id_map[journal['identifiers']] = journal_internal_id

            # Create a DataFrame for each category in the journal
            for category in journal['categories']:
                # Generate a unique internal ID for the category
                category_internal_id = self.generate_internal_id()
                category_id_map[category['id']] = category_internal_id
                categories.append({
                    'internal_id': category_internal_id,
                    'journal_internal_id': journal_internal_id,
                    'category_id': category['id'],
                    'quartile': category.get('quartile', None)
                })
            # I have done this so more journals can have the same objects ("categories, areas")
            # Create a DataFrame for areas
            for area in journal['areas']:
                # Generate a unique internal ID for the area
                area_internal_id = self.generate_internal_id()
                area_id_map[area] = area_internal_id
                areas.append({
                    'internal_id': area_internal_id,
                    'journal_internal_id': journal_internal_id,
                    'area': area
                })
            
            # Add journal information
            journals.append({
                'internal_id': journal_internal_id,
                'identifiers': journal['identifiers'],
            })
        
        # Convert to DataFrames
        journal_df = DataFrame(journals)
        category_df = DataFrame(categories)
        area_df = DataFrame(areas)
        
        # Connect to SQLite database and insert data


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
