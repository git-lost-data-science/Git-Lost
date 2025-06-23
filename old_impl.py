# import csv # * reader
import numpy as np
import json # * load
import re
from pprint import pprint 
from typing import Optional, Self

import rdflib
import pandas as pd  # * DataFrame, Series
import sqlite3 # * connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import sparql_dataframe

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

class IdentifiableEntity:
    def __init__(self, id: object):
        if not (isinstance(id, list) or not (isinstance(id, str)) and all(isinstance(value, str) for value in id)):
            raise TypeMismatchError("a list of strings or a string", id)
        self.id: list[str] | str = id 

    def getIds(self):
        return list(self.id)
    
    def __eq__(self, other: Self) -> bool: # checking for value equality
        return self.id == other.id 
    
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
    def __init__(self): # Martina: I added the parameter dbPathOrUrl: str --> since it is a parameter we're defining in this case I think we should have it here
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
    # def __init__(self, dbPathOrUrl): # Adding this I think it actually specifies what it takes (dbPathOrUrl)
    #     super().__init__(dbPathOrUrl)  # ? I think we don't need this actually because it inherently inherits Handler's parameters

    def pushDataToDb(self, path: str): #-> bool: 
        if path.endswith(".json"):  # Here I changed it to dbPathOrUrl for better reconnection between classes (imo), but maybe even better would be to use self.getDbPathOrUrl
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
            #graph_endp = "http://127.0.0.1:9999/blazegraph/sparql"
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
        Category = rdflib.URIRef("https://schema.org/CategoryClass")                       
        Area = rdflib.URIRef("https://schema.org/AreaClass")  
    
        # referencing the attributes:
        id = rdflib.URIRef("https://schema.org/identifier")
        title = rdflib.URIRef("https://schema.org/name")
        languages = rdflib.URIRef("https://schema.org/inLanguage") # (superseded /Language)
        publisher = rdflib.URIRef("https://schema.org/publisher")
        seal = rdflib.URIRef("https://schema.org/hasDOAJSeal") # invented
        license = rdflib.URIRef("https://schema.org/license")
        apc = rdflib.URIRef("https://schema.org/hasAPC") # this is a boolean value in theory so it should work.
        
        
        # referencing the relations: 
        hasCategory = rdflib.URIRef("http://purl.org/dc/terms/isPartOf")
        hasArea = rdflib.URIRef("http://purl.org/dc/terms/subject") # the hasArea better fits the idea of 'subject' rather than hasCategory
            
        #csv_file = "/data_science_project/doaj.csv"
        journals = pd.read_csv(csv_file, 
                           keep_default_na=False, 
                           dtype={
                                  'Journal title': 'str', 
                                  'Languages in which the journal accepts manuscripts': 'str', 
                                  'Journal ISSN (print version)': 'str',
                                  'Journal EISSN (online version)': 'str',
                                  'Publisher': 'str',
                                  'DOAJ Seal': 'str',
                                  'Journal license': 'str',
                                  'APC': 'str' 
                           })
        
        journals = journals.rename(columns={'Journal title': 'title', 
                                             'Languages in which the journal accepts manuscripts': 'languages', 
                                             'Journal ISSN (print version)': 'issn',
                                             'Journal EISSN (online version)': 'eissn',
                                             'Publisher': 'publisher',
                                             'DOAJ Seal': 'seal',
                                             'Journal license': 'license',
                                             'APC': 'apc'})
        # I added the next two lines that should fix the error of pandas not being able to convert safely strings
        # or other kind of conflicting and mixed dtypes in the column of APC and DOAJ to 'boolean' type of values.
        journals['apc'] = journals['apc'].str.lower().map({'yes': True,'no': False}).fillna(False).astype('bool')
        journals['seal'] = journals['seal'].str.lower().map({'yes': True, 'no': False}).fillna(False).astype('bool')
        
        journal_ids_list = []
        for _, row in journals.iterrows(): 
            issn = str(row.get("issn", "")).strip()
            eissn = str(row.get("eissn", "")).strip()

            if issn and eissn:
                combined_id = f"{issn},{eissn}"
            elif issn and not eissn:
                combined_id = issn
            elif eissn and not issn:
                combined_id = eissn
            else:
                combined_id = ""
            journal_ids_list.append(combined_id)

        journals.insert(0, 'journal-ids', journal_ids_list)
        journals = journals.drop(columns=['issn','eissn'])

        base_url = "https://github.com/git-lost-data-science/res/"
            
        journals_int_id = {}
        for idx, row in journals.iterrows():
            lang_str = ', '.join(lang for lang in row['languages'])
            
            loc_id = "journal-" + str(idx)
    
            subj = rdflib.URIRef(base_url + loc_id)
    
            journals_int_id[row['title']] = subj
    
            j_graph.add((subj, rdflib.RDF.type, Journal))
            j_graph.add((subj, id, rdflib.Literal(row['journal-ids'])))
            j_graph.add((subj, title, rdflib.Literal(row['title'])))
            j_graph.add((subj, languages, rdflib.Literal(lang_str)))
            j_graph.add((subj, publisher, rdflib.Literal(row['publisher'])))
            j_graph.add((subj, seal, rdflib.Literal(row['seal'])))    
            j_graph.add((subj, license, rdflib.Literal(row['license'])))
            j_graph.add((subj, apc, rdflib.Literal(row['apc']))) 
            j_graph.add((subj, hasCategory, Category)) # category_int_id[row['categories']]))  # HERE I'm technically missing just the @quartile
            j_graph.add((subj, hasArea, Area)) # area_int_id[row['area']]))
        return j_graph

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
            return categories_df 
        
# TODO FOR THE END OF APRIL (hopefully)

# QUERY HANDLER 
class QueryHandler(Handler):  
    def getById(id: str) -> pd.DataFrame: 
        pass # Ila

class CategoryQueryHandler(QueryHandler): 
    def __init__ (self):
        super().__init__()

    def getAllCategories() -> pd.DataFrame: # Rumana
        pass
    
    def getAllAreas(self) -> pd.DataFrame: # Martina
        # SELECT area FROM categories; This is the query in itself. 
        # TO BE MODIFIED in order to not have repetitions (so only the first instance is restituted).
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                q2="SELECT DISTINCT area FROM categories;" # DISTINCT allows to avoid showing duplicates.
                q2_df = pd.read_sql(q2, con)
                return q2_df
        except Exception as e:
                print(f"Connection to SQL database failed due to error: {e}") 
                return pd.DataFrame() # in order to always return a DataFrame object, even if the queries fails for some reason.   
    
    def getCategoriesWithQuartile(quartiles:set[str]) -> pd.DataFrame: # Nico
        pass
    def getCategoriesAssignedToAreas(area_ids: set[str]) -> pd.DataFrame: # Ila
        pass
    def getAreasAssignedToCategories(categrory_ids: set[str]) -> pd.DataFrame: # Rumana
        pass

class JournalQueryHandler(QueryHandler): # all the methods return a DataFrame
    def getAllJournals(self): # Martina
        try:
            endpoint = self.getDbPathOrUrl()
            journal_query = '''
            PREFIX res:    <https://github.com/git-lost-data-science/res/>
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT *
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s ?p ?o .
            }
            ''' # Though I think raising a SELECT with * might be potentially perilous 
            
            journal_df = sparql_dataframe.get(endpoint, journal_query, True)
            return journal_df
        
        except Exception as e:
            print(f"Error in SPARQL query due to: {e}") 
            return pd.DataFrame()
    
    def getJournalsWithTitle(self, partialTitle: str): # Nico
        try:
            endpoint = self.getDbPathOrUrl()        
            query = f"""
            PREFIX res:    <https://github.com/git-lost-data-science/res/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
            WHERE {{ # ? using the standard parameters required
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?id .
                ?s schema:title ?title .
                ?s schema:inLanguage ?languages .
                ?s schema:publisher ?publisher .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
                ?s schema:doajSeal ?seal .

                FILTER CONTAINS(LCASE(STR(?title)), LCASE("{partialTitle}")) # ? matching the title and the partial title
            }}
            """

            titles_df = sparql_dataframe.get(endpoint, query, True) # modified self.getDbPathOrUrl to endpoint (because we had already defined it)
            return titles_df
        
        except Exception as e:
            print(f"Error in the SPARQL query: {e}")
            return pd.DataFrame

    def getJournalsPublishedBy(self, partialName: str): #Ila
        pass
    def getJournalsWithLicense(self, licenses: set[str]): # Rumana
        pass
    def getJournalsWithAPC(self): # Martina
        try:
            endpoint = self.getDbPathOrUrl()
            jouAPC_query = '''
            PREFIX res:    <https://github.com/git-lost-data-science/res/>
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?publisher ?languages ?seal ?license ?apc
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?id .
                ?s schema:name ?title .
                ?s schema:publisher ?publisher .
                ?s schema:inLanguage ?languages .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
                FILTER (?apc = true)
            }
            ''' 
            # After a million times of loading the data the query works correctly filtering ONLY the Journals with APC (apc = true)
            # if this was actually what was required...
            
            jouAPC_df = sparql_dataframe.get(endpoint, jouAPC_query, True)
            return jouAPC_df
        
        except Exception as e:
            print(f"Error in SPARQL query due to: {e}") 
            return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self): # Nico
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
            PREFIX res:    <https://github.com/git-lost-data-science/res/>
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?publisher ?languages ?license ?apc ?seal
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?id .
                ?s schema:name ?title .
                ?s schema:publisher ?publisher .
                ?s schema:inLanguage ?languages .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
                ?s schema:hasDOAJSeal ?seal .
                FILTER (?seal = true)
            }
            """
            
            journal_DOAJ_df = sparql_dataframe.get(endpoint, query, True)
            return journal_DOAJ_df
        
        except Exception as e:
            print(f"The query was unsuccessful due to the following error: {e}") 
            return pd.DataFrame
        
class BasicQueryEngine(): # To be implemented the journalQuery and categoryQuery attributes (DONE)
    def __init__(self):
        self.journalQuery = []  # Martina: presumably empty lists here?
        self.categoryQuery = []

    def cleanJournalHandlers(self) -> bool:  # Rumana
        self.journalQuery = []
        return True
    
    def cleanCategoryHanders(self) -> bool: # Ila
        self.categoryQuery = []
        return True
# here I think we need to create the graphs and 'clean' the DF to see if there are doubles, if the years (and numbers in general) are floats, etc. 
    
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool: # Martina: this presumably adds the methods to handle Journal queries to journalQuery list
        try:
            self.journalQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to: {e}")
            return False
        
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool: # Nico 
        try:
            self.categoryQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the category handlers to the categoryQuery

    def getEntityById(self, id:str) -> Optional[IdentifiableEntity]: # Rumana
        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(, \d{4}-\d{3,4}X?)*$')

        if bool(journal_id_pattern.match(id)): # yes, it is a journal
            journals = list(set(journal.getById(id) for journal in self.journalQuery)) 
            categories = list(set(category.getById(id) for category in self.categoryQuery))

            journal_categories = []
            journal_areas = []

            for _, row in categories.iterrows():
                category = Category(row.get("category"), row.get("quartile"))
                area = Area(row.get("area"))
                journal_categories.append(category)
                journal_areas.append(area)

            for _, row in journals.iterrows():
                return Journal(id, row.get("title"), row.get("languages"), row.get("publisher"), row.get("seal"), 
                               row.get("licence"), row.get("apc"), Optional[category], Optional[area])
        else:
            categories = list(set(category.getById(id) for category in self.categoryQuery))
            if "category" in categories.columns:
                for _, row in categories.iterrows():
                    return Category(row.get("category"), row.get("quartile"))
            elif "area" in categories.columns:
                return Area(row.get("area"))
            else:
                return None

    def getAllJournals() -> list[Journal]: # Ila
        pass
    def getJournalsWithTitle(self, partialTitle: str) ->list[Journal]: # Martina -- they have to match, even partially, with the input string
        all_jou = []
        result = []

        for journalQueryHandler in self.journalQuery: 
            journal_df = journalQueryHandler.getJournalsWithTitle(partialTitle)
            all_jou.append(journal_df)

        if not all_jou.empty():
            db = pd.concat(all_jou, ignore_index=True).drop_duplicates().fillna('')
            partialTitle = partialTitle.replace('"', '\\"')   # trying this thing
            
            for _, row in db.iterrows():
                if db['title'].astype(str).str.lower().str.contains(partialTitle, na=False):
                    j_id = row.get('journal-ids')
                    jou_obj = self.getEntityById(j_id)
                    result.append(jou_obj)
            return result
        else:
            return []
        
    def getJournalsPublishedBy(partialName:str) -> list[Journal]: # Nico
        pass
    def getJournalsWithLicense(licenses:set[str]) -> list[Journal]: # Rumana
        pass
    def getJournalsWithAPC() -> list[Journal]: # Ila 
        pass
    def getJournalsWithDOAJSeal(self) -> list[Journal]: # Martina -- all those one that specify a DOAJ seal (meaning they have it(=True) or in general?)
        jou_seal = []
        res_obj = []

        for journalQueryHandler in self.journalQuery: 
            journal_df = journalQueryHandler.getJournalsWithDOAJSeal()
            jou_seal.append(journal_df)

        if not jou_seal.empty():
            db = pd.concat(jou_seal, ignore_index=True).drop_duplicates().fillna('')
            db['seal'] = db['seal'].astype(bool) # ensuring it is treated as a boolean type if anything happens
            
            for _, row in db.iterrows():
                if row['seal'] == True: 
                    j_id = row.get('journal-ids')
                    jou_obj = self.getEntityById(j_id)
                    res_obj.append(jou_obj) 
            return res_obj
        else:
            return []

    def getAllCategories() -> list[Category]: # Nico 
        pass
    def getAllAreas() -> list[Area]: # Rumana
        pass
    def getCategoriesWithQuartile(quartiles:set[str]) -> list[Category]: # Ila
        pass
    def getCategoriesAssignedToAreas(self, areas_ids: set[str]) -> list[Category]: # Martina -- returns all the areas that are specified by the category in input (no repetitions)
        cat_areas = []
        assigned_cat = []

        for categoryQueryHandler in self.categoryQuery: 
            category_df = categoryQueryHandler.getCategoriesAssignedToAreas({areas_ids})
            cat_areas.append(category_df)

        if not cat_areas.empty():
            db = pd.concat(cat_areas, ignore_index=True).drop_duplicates().fillna('')
            
            norm_areas = {str(a_id).lower() for a_id in areas_ids}
            seen_categories = set()
            for _, row in db.iterrows():
                area = (str(area).lower() for area in row['area'])
                if area.isin(norm_areas, na=False):
                    cat_id = row.get('category')
                    category = self.getEntityById(cat_id)
                    if category not in seen_categories:
                        seen_categories.add(category)
                        assigned_cat.append(category)
            return assigned_cat

        else:
            return []
      
        # if cat_areas:
        #     db = pd.concat(cat_areas, ignore_index=True).drop_duplicates()
        #     db = db[['internal-id', 'journal-ids', 'category', 'quartile', 'area']].fillna('')

        #     #areas_ids = areas_ids.astype(str).split(',')
            
            
        #     match = db['area'].astype(str).str.lower().str.contains(area, na=False) 
        #     matching_db = db[match]

        #     for idx, row in matching_db.iterrows():
        #         if cat_obj not in assigned_cat:  # technically avoiding repetitions in the list returned (?)
        #             cat_obj = Category(
        #                 id = row.get('id'),
        #                 quartile = row.get('quartile')
        #             )
        #             assigned_cat.append(cat_obj)
        #     return assigned_cat
        # else:
        #     return []

    def getAreasAssignedToCategories(category_ids: set[str]) -> list[Area]: # Nico 
        pass

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]): # Rumana
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str]): # Ila 
        pass
    def getDiamondJournalsAreasAndCategoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]): # Martina
        # returns only journals that DO NOT HAVE an APC -- NO REPETITIONS
        # IF EMPTY input --> ALL things are specified 
        # at least ONE of the input CATEGORY (with the related quartileS) and at least ONE AREA
        pass



