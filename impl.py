# import csv # * reader
import numpy as np
import json # * load
import re
from pprint import pprint 
from typing import Optional

import rdflib
import pandas as pd  # * DataFrame, Series
import sqlite3 # * connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 

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

class Area(IdentifiableEntity): 
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
        if self.dbPathOrUrl:
            self.dbPathOrUrl = pathOrUrl
            
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
            jou_graph = self._csv_file_to_graph(path)
            try:
                store = SPARQLUpdateStore()
                store.open((self.dbPathOrUrl, self.dbPathOrUrl))
    
                for triple in jou_graph.triples((None, None, None)):  
                    store.add(triple)                           
               
                store.close()    
                return True
            except Exception as e:
                print(f"Error uploading data: {e}")
                return False  
            # pass # TODO Pushing the data of the CSV (Martina and Rumana)

        else: 
            return False # ? This case must be included

    def _json_file_to_df(self, _: str) -> pd.DataFrame:
        ... # ? needed; defined in the CategoryUploadHandler
    
    def _csv_file_to_graph(self, _: str) -> rdflib.Graph:
        ... # ? needed; defined in the JournalUploadHandler


class JournalUploadHandler(UploadHandler):
    def _csv_file_to_graph(self, csv_file: str):
        # initiating an empty graph:
        j_graph = rdflib.Graph()
            
        # referencing all the classes:    
        Journal = rdflib.URIRef("https://schema.org/Periodical")   
        Category = rdflib.URIRef("https://schema.org/category")                       
        Area = rdflib.URIRef("https://schema.org/subjectOf")  
    
        # referencing the attributes:
        title = rdflib.URIRef("https://schema.org/title") # ? Ila changed this to title 
        issn = rdflib.URIRef("https://schema.org/issn") # ? ila added this
        eissn= rdflib.URIRef("https://schema.org/eissn") # ? ila added this
        languages = rdflib.URIRef ("https://schema.org/inLanguage") # (superseded /Language)
        publisher = rdflib.URIRef("https://schema.org/publisher")
        seal = rdflib.URIRef("http://doaj.org/static/doaj/doajArticles.xsd")    
        license = rdflib.URIRef("https://schema.org/license")
        apc = rdflib.URIRef("https://schema.org/isAccessibleForFree") # this is a boolean value in theory so it should work.
        
        # referencing the relations: 
        hasCategory = rdflib.URIRef("http://purl.org/dc/terms/isPartOf")
        hasArea = rdflib.URIRef("http://purl.org/dc/terms/subject") # the hasArea better fits the idea of 'subject' rather than hasCategory
            
        journals = pd.read_csv(csv_file, 
                           keep_default_na=False, 
                           dtype={
                                  'Journal title': 'string', 
                                  'Journal ISSN (print version)': 'string',
                                  'Journal EISSN (online version)': 'string',
                                  'Languages in which the journal accepts manuscripts': 'string', 
                                  'Publisher': 'string',
                                  'DOAJ Seal': 'bool',
                                  'Journal license': 'string',
                                  'APC': 'bool' 
                           })
        journals = journals.rename(columns={'Journal title': 'title', 
                                            'Journal ISSN (print version)': 'id-print',
                                            'Journal EISSN (online version)': 'id-online',
                                             'Languages in which the journal accepts manuscripts': 'languages', 
                                             'Publisher': 'publisher',
                                             'DOAJ Seal': 'seal',
                                             'Journal license': 'license',
                                             'APC': 'apc'})
    
        base_url = "https://github.com/git-lost-data-science/res"
            
        journals_int_id = {}
        for idx, row in journals.iterrows():
            loc_id = "journal-" + str(idx)
    
            subj = rdflib.URIRef(base_url + loc_id)
    
            journals_int_id[row['title']] = subj
    
            j_graph.add((subj, rdflib.RDF.type, Journal))
            j_graph.add((subj, title, rdflib.Literal(row['title'])))
            j_graph.add((subj, issn, rdflib.Literal(row['id-print'])))
            j_graph.add((subj, eissn, rdflib.Literal(row['id-online'])))
            j_graph.add((subj, languages, rdflib.Literal(row['languages'].split(','))))
            j_graph.add((subj, publisher, rdflib.Literal(row['publisher'])))
            j_graph.add((subj, seal, rdflib.Literal(row['seal'])))    
            j_graph.add((subj, license, rdflib.Literal(row['license'])))
            j_graph.add((subj, apc, rdflib.Literal(row['apc']))) 
            # THIS PART IS ADDED AFTERWARDS --    
            j_graph.add((subj, hasCategory, Category)) # category_int_id[row['categories']]))  # HERE I'm technically missing just the @quartile
            j_graph.add((subj, hasArea, Area)) # area_int_id[row['area']]))
            # -- THIS PART IS ADDED AFTWERWARDS. 
        # print(len(j_graph))
        return j_graph

class CategoryUploadHandler(UploadHandler): 

    def _json_file_to_df(self, json_file: str) -> pd.DataFrame: 
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
            return categories_df 

# QUERY HANDLER 
class QueryHandler(Handler): # ! done, but Ila needs to test it 
    def __init__(self):
        super().__init__()  

    def getById(self, id: str) -> pd.DataFrame: 
        path = self.dbPathOrUrl
        if path.endswith(".db"): 
            try:
                with sqlite3.connect(path) as con: # LIKE is used also to match partial strings 
                    query = """
                        SELECT * FROM Category
                        WHERE LOWER([internal-id]) = LOWER(?)
                           OR LOWER([journal-ids]) LIKE LOWER(?) 
                           OR LOWER([category]) = LOWER(?) 
                           OR LOWER([quartile]) = LOWER(?)
                           OR LOWER([area]) = LOWER(?)
                    """ # ";" not necessary on python 
                    params = (id.lower(), f"%{id.lower()}%", id.lower(), id.lower(), id.lower()) # '?' are placeholders, % are wildcards, before and after it may contain other characters, that's why 
                    cat_df = pd.read_sql(query, con, params=params) 
                    return cat_df
            except Exception as e:
                print(f"Error in the query: {e}")
                return pd.DataFrame()
            
        elif re.match(r"^https?://[a-zA-Z0-9.-]+(?:\:\d+)?/blazegraph(?:/[\w\-./]*)?/sparql$", path):
            endpoint= self.dbPathOrUrl
            query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            
            SELECT ?internalId ?title ?issn ?eissn ?languages ?publisher ?seal ?license ?apc
            WHERE {{
                ?internalId rdf:type schema:Periodical .
                ?internalId schema:title ?title .
                ?internalId schema:issn ?issn .
                ?internalId schema:eissn ?eissn .
                ?internalId schema:inLanguage ?languages .
                ?internalId schema:publisher ?publisher .
                ?internalId schema:license ?license .
                ?internalId schema:isAccessibleForFree ?apc .
                ?internalId schema:doajSeal ?seal .
                
                FILTER (
                    LCASE(?title) = LCASE("{id}") || 
                    LCASE(?issn) = LCASE("{id}") ||
                    LCASE(?eissn) = LCASE("{id}")
                )
            }}
            """
            # query SPARQL
            try:
                df = get(endpoint, query, True)
                return df
            except Exception as e:
                print(f"Error in SPARQL query: {e}")
                return pd.DataFrame()
        else: 
            return pd.DataFrame()


class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllCategories() -> pd.DataFrame: # Rumana
        pass
    def getAllAreas() -> pd.DataFrame: # Martina
        pass
    def getCategoriesWithQuartile(self, quartiles:set[str]) -> pd.DataFrame: # Nico
        pass
    
    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame: # ! Ila, done, needs to be tested
        categories = []
        path = self.dbPathOrUrl
        try:
            # sqlite db
            with sqlite3.connect(path) as con:
                # for every area_id
                for area_id in area_ids:
                    query = """
                        SELECT area, category
                        FROM Category
                        WHERE area = ?
                        ;
                    """
                    # query
                    df = pd.read_sql(query, con, params=(area_id,)) # there is a comma because it is a tuple
                    categories.append(df)
            # merge all the obtained df
            all_categories = pd.concat(categories, ignore_index=True)
            return all_categories
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame()  
        
    def getAreasAssignedToCategories(categrory_ids: set[str]) -> pd.DataFrame: # Rumana
        pass

class JournalQueryHandler(QueryHandler): # all the methods return a DataFrame
    def __init__():
        super().__init__()
    def getAllJournals(): # Martina
        pass
    def getJournalsWithTitle(self, partialTitle: str): # Nico
        pass
    def getJournalsPublishedBy(self, partialName: str): # ! Ila done, needs to be tested 
        endpoint = self.dbPathOrUrl
        # i dind't use the prefix for dc, check
        query = f"""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?internalId ?title ?issn ?eissn ?languages ?publisher ?seal ?license ?apc
        WHERE {{
            ?internalId rdf:type schema:Periodical .
            ?internalId schema:title ?title .
            ?internalId schema:issn ?issn .
            ?internalId schema:eissn ?eissn .
            ?internalId schema:inLanguage ?languages .
            ?internalId schema:publisher ?publisher .
            ?internalId schema:license ?license .
            ?internalId schema:isAccessibleForFree ?apc .
            ?internalId schema:doajSeal ?seal .

            FILTER CONTAINS(LCASE(STR(?publisher)), LCASE("{partialName}"))
        }}
        """
        try:
            df = get(endpoint, query, True)
            return df
        except Exception as e:
            print(f"Error in SPARQL query: {e}")
            return pd.DataFrame()

    def getJournalsWithLicense(self, licenses: set[str]): # Rumana
        pass
    def JournalsWithAPC(): # Martina
        pass
    def JournalsWithDOAJSeal(): # Nico
        pass
        
class BasicQueryEngine(object):
    def __init__(self): # Ila, done
        self.journalQuery= []
        self.categoryQuery= []

    def cleanJournalHandlers(self) -> bool:
        self.journalQuery= []
        return True
    def cleanCategoryHandlers(self) -> bool: #  Ila, done
        self.categoryQuery= []
        return True
        
    def addJournalHandler(handler: JournalQueryHandler) -> bool: # Martina
        pass
    def addCategoryHandler(handler: CategoryQueryHandler) -> bool: # Nico 
        pass
    def getEntityById(id:str) -> Optional[IdentifiableEntity]: # Rumana
        pass
    def getAllJournals() -> list[Journal]: # Ila
        pass
    def getJournalsWithTitle(partialTitle:str) ->list[Journal]: # Martina
        pass
    def getJournalsPublishedBy(partialName:str) -> list[Journal]: # Nico
        pass
    def getJournalsWithLicense(licenses:set[str]) -> list[Journal]: # Rumana
        pass
    def JournalsWithAPC() -> list[Journal]: # Ila 
        pass
    def getJournalsWithDOAJSeal() -> list[Journal]: # Martina
        pass
    def getAllCategories() -> list[Category]: # Nico 
        pass
    def getAllAreas() -> list[Area]: # Rumana
        pass
    def getCategoriesWithQuartile(quartiles:set[str]) -> list[Category]: # Ila
        pass
    def getCategoriesAssignedToAreas(areas_ids: set[str]) -> list[Category]: # Martina
        pass
    def getAreasAssignedToCategories(category_ids: set[str]) -> list[Area]: # Nico 
        pass

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]): # Rumana
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str]): # Ila 
        pass
    def getDiamondJournalsAreasAmdCAtegoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]): # Martina
        pass

