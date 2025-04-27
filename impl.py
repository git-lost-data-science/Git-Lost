# import csv # * reader
import numpy as np
import json # * load
import re
from pprint import pprint 
from typing import Optional

import pandas as pd  # * DataFrame, Series
import sqlite3 # * connect

# ? I (Nico) have fixed imports
# ? Having this import setup makes it very clear which functions and classes belong to which libraries
# ? The first paragraph consists solely of modules that are included in the base installation of Python
# ? The second paragraph is made up of libraries that require installation

# ? I have made this class as a simpler means of error handling and to avoid duplication
class TypeMismatchError(Exception):
    def __init__(self, expected_type_description: str, obj: object):
        actual_type_name = type(obj).__name__
        preposition = "an" if actual_type_name[0] in "aeiou" else "a"
        super().__init__(f"Expected {expected_type_description}, got {preposition} {actual_type_name}.")

class IdentifiableEntity():
    def __init__(self, id: object):
        if not (isinstance(id, list) and all(isinstance(value, str) for value in id)) or not isinstance(id, str):
            raise TypeMismatchError("a list of strings or a string", id)
        self.id: list[str] | str = id 

    def getIds(self):
        return list(self.id)
    
class Category(IdentifiableEntity):
    def __init__(self, id, quartile: Optional[str]): 
        super().__init__(id) 
        if quartile is not None and not isinstance(quartile, str):
            raise TypeMismatchError("a NoneType or str", quartile)
        self.quartile = quartile 
        
    def getQuartile(self): 
        return self.quartile 

class Area(IdentifiableEntity): # ? 0 or more. Nothing to add, inherits the methods of the super()
    pass 

class Journal(IdentifiableEntity):
    def __init__(self, id, title: str, languages: str|list, publisher: Optional[str], 
                 seal: bool, licence: str, apc: bool, hasCategory: Optional[list[Category]], 
                 hasArea: Optional[list[Area]]):
        super().__init__(id)

        if not isinstance(title, str) or not title:
            raise TypeMismatchError("a non-empty str", title) # ? the same as raise TypeError(f"Expected a non-empty str, got {type(title).__name__}")
        
        if not isinstance(languages, list) or not all(isinstance(lang, str) for lang in languages) or not languages:
            raise TypeMismatchError("a non-empty str or list", languages)
        
        if not (isinstance(publisher, str) or isinstance(publisher, None)):
            raise TypeMismatchError("a str or a NoneType", publisher)
        
        if not isinstance(seal, bool):
            raise TypeMismatchError("a boolean", seal)
        
        if not isinstance(licence, str) or not licence:
            raise TypeMismatchError("a non-empty str", licence)
        
        if not isinstance(apc, bool):
            raise TypeMismatchError("a boolean", apc)
        
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

class Handler: 
    def __init__(self):
        self.dbPathOrUrl = "" # When initialised, there is no set database path or URL

    def getDbPathOrUrl(self): # @property 
        return self.dbPathOrUrl 
     
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:  # setter
        if not pathOrUrl.strip(): 
            return False
        if pathOrUrl.endswith(".db"): # if it is a local path: it is valid
            self.dbPathOrUrl = pathOrUrl
            return True
        elif re.match(r"^https?://[a-zA-Z0-9.-]+(?:\:\d+)?/blazegraph(?:/[\w\-./]*)?/sparql$", 
                        pathOrUrl): # if it is a blazegraph url: valid
            self.dbPathOrUrl = pathOrUrl
            return True
        return False 

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str) -> bool: 
        if path.endswith(".json"):
            categories_df = self._json_file_to_df(path) # ? running the function without directly requiring the creation of the instance of a class 
            try: 
                with sqlite3.connect(self.dbPathOrUrl) as con:    
                    categories_df.to_sql("Categories", con, if_exists="replace", index=False)
                    con.commit()
                return True
            except Exception as e:
                print(f"Error uploading data: {e}")
                return False       
        elif path.endswith(".csv"): # ? This case MUST be included because the file can't just be assumed to be CSV
            try: # TODO Pushing the data of the CSV (Martina and Rumana)
                pass
            except Exception as e:
                print(f"Error uploading data: {e}")
                return False  
        else: 
            return False # ? This case must be included

    def _json_file_to_df(self, _: str) -> pd.DataFrame:
        ... # ? needed; defined in the CategoryUploadHandler


class JournalUploadHandler(UploadHandler):
    pass

class CategoryUploadHandler(UploadHandler): 
    # ? How does this work?
    # ? 1. An instance of this class is initalised
    # ? 2. The method is called in the pushDataToDB class to produce the dataframe

    def _json_file_to_df(self, json_file: str) -> pd.DataFrame: # the string path
        # * Some changes here !!!
        # * 1. Labelling the objects as 'self' does not make any sense
        # * 2. I have changed the journal-id label to journal-ids because there are cases where 
        # * there are multiple journal ids
        # * 3. I use np.nan as the second argument of the .get function for the quartile line

        with open(json_file, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            json_df = pd.DataFrame(json_data) 

            df_identifiers = json_df[["identifiers"]] 
            internal_ids = []
            for idx, row in df_identifiers.iterrows():
                internal_ids.append("cat-" + str(idx)) 

            json_df.insert(0, "internal-id", pd.Series(internal_ids, dtype="string")) 
            json_df = json_df.rename(columns={"identifiers": "journal-ids"}) 

            rows = []
            for _, row in json_df.iterrows():
                journal_ids = row["journal-ids"]  
                internal_id = row["internal-id"] 
                categories = row["categories"]
                areas = row["areas"]

                for cat in categories: 
                    for area in areas: 
                        rows.append({
                            "internal-id": internal_id,  
                            "journal-ids": ', '.join(journal_ids),  
                            "category": cat["id"],  
                            "quartile": cat.get("quartile", np.nan), 
                            "area": area
                        })
            
            categories_df = pd.DataFrame(rows)
            # Ila: categories_df["quartile"] = self.categories_df["quartile"].fillna("N/A") 
            return categories_df 
        
# TODO BY THE END OF APRIL (hopefully)

# QUERY HANDLER 
class QueryHandler(Handler):  
    def getById(id: str) -> pd.DataFrame: 
        pass # Ila

class CategoryQueryHandler(QueryHandler):
    def getAllCategories() -> pd.DataFrame: # Rumana
        pass
    def getAllAreas() -> pd.DataFrame: # Martina
        pass
    def getCategoriesWithQuartile(quartiles:set[str]) -> pd.DataFrame: # Nico
        pass
    def getCategoriesAssignedToAreas(area_ids: set[str]) -> pd.DataFrame: # Ila
        pass
    def getAreasAssignedToCategories(categrory_ids: set[str]) -> pd.DataFrame: # Rumana
        pass

class JournalQueryHandler(): # all the methods return a DataFrame
    pass
    def getAllJournals(): # Martina
        pass
    def getJournalsWithTitle(self, partialTitle: str): # Nico
        pass
    def getJournalsPublishedBy(self, partialName: str): #Ila
        pass
    def getJournalsWothLicense(self, licenses: set[str]): # Rumana
        pass
    def JournalsWithAPC(): # Martina
        pass
    def JournalsWithDOAJSeal(): # Nico
        pass
        
# Ila changed this: I just added all the functions 
class BasicQueryEngine(object):
    pass
    def cleanJournalHandlers() -> bool:
        pass
    def cleanCategoryHanders() -> bool: 
        pass
# Ila: here I think we need to create the graphs and 'clean' the DF to see if there are doubles, if the years (and numbers in general) are floats, etc. 
    
    def addJournalHandler(handler: JournalQueryHandler) -> bool:
        pass
    def addCategoryHandler(handler: CategoryQueryHandler) -> bool: 
        pass
    def getEntityById(id:str) -> Optional[IdentifiableEntity]:
        pass
    def getAllJournals() -> list[Journal]:
        pass
    def getJournalsWithTitle(partialTitle:str) ->list[Journal]:
        pass
    def getJournalsPublishedBy(partialName:str) -> list[Journal]:
        pass
    def getJournalsWithLicense(licenses:set[str]) -> list[Journal]:
        pass
    def JournalsWithAPC() -> list[Journal]:
        pass
    def getJournalsWithDOAJSeal() -> list[Journal]:
        pass
    def getAllCategories() -> list[Category]:
        pass
    def getAllAreas() -> list[Area]:
        pass
    def getCategoriesWithQuartile(quartiles:set[str]) -> list[Category]:
        pass
    def getCategoriesAssignedToAreas(areas_ids: set[str]) -> list[Category]:
        pass
    def getAreasAssignedToCategories(category_ids: set[str]) -> list[Area]:
        pass

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]) -> list[Journal]: 
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str])-> list[Journal]: 
        pass
    def getDiamondJournalsAreasAmdCAtegoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str])-> list[Journal]:
        pass
