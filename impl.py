import numpy as np
import json # * load
import re
from pprint import pprint 
from typing import Optional, Self
import os

import rdflib
import pandas as pd  # * DataFrame, Series
import sqlite3 # * connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import sparql_dataframe 

# ! BLAZEGRAPH: java -server -Xmx1g -jar blazegraph.jar

class TypeMismatchError(Exception):
    def __init__(self, expected_type_description: str, obj: object):
        actual_type_name = type(obj).__name__
        preposition = "an" if actual_type_name[0] in "aeiou" else "a"
        super().__init__(f"Expected {expected_type_description}, got {preposition} {actual_type_name}.")

class IdentifiableEntity:
    def __init__(self, id: object): 
        if not (isinstance(id, list) and all(isinstance(value, str) for value in id)) and not isinstance(id, str):
            raise TypeMismatchError("a list of strings or a string", id)
        self.id: list[str] | str = id 

    def __eq__(self, other: Self) -> bool: # checking for value equality
        return self.id == other.id 
    
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
    def __init__(self, id): 
        super().__init__(id) 

    # def __repr__(self): # ! testing purposes only
        # return f"{self.__class__.__name__}(id={self.id})"

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
        # Are has hasCategory and hasArea needed here?

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
        elif "blazegraph" in pathOrUrl: # if it is a blazegraph url: valid
            self.dbPathOrUrl = pathOrUrl # ! change this to a regular expression !
            return True
        return False 

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str) -> bool:
        absolute_path = os.path.abspath(path) # Ila: os uses always an absolute path
        if not os.path.exists(absolute_path):
            return False

        if path.endswith(".json"):
            categories_df = self._json_file_to_df(absolute_path)
            try:
                with sqlite3.connect(self.dbPathOrUrl) as con:
                    categories_df.to_sql("Category", con, if_exists="replace", index=False)
                    con.commit()
                return True
            except:
                return False

        elif path.endswith(".csv"):
            jou_graph = self._csv_file_to_graph(absolute_path)
            try:
                store = SPARQLUpdateStore()
                store.open((self.dbPathOrUrl, self.dbPathOrUrl))
                for triple in jou_graph.triples((None, None, None)):
                    store.add(triple)
                store.close()
                return True
            except:
                return False
        else:
            return False

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
        title = rdflib.URIRef("https://schema.org/name")
        languages = rdflib.URIRef("https://schema.org/inLanguage") # (superseded /Language)
        issn = rdflib.URIRef("https://schema.org/issn")
        eissn = rdflib.URIRef("https://schema.org/eissn") # invented
        publisher = rdflib.URIRef("https://schema.org/publisher")
        seal = rdflib.URIRef("https://schema.org/hasDOAJSeal") # invented
        license = rdflib.URIRef("https://schema.org/license")
        apc = rdflib.URIRef("https://schema.org/isAccessibleForFree") # this is a boolean value in theory so it should work.
        
        
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

        base_url = "https://github.com/git-lost-data-science/res/"
            
        journals_int_id = {}
        for idx, row in journals.iterrows():
            loc_id = "journal-" + str(idx)
    
            subj = rdflib.URIRef(base_url + loc_id)
    
            journals_int_id[row['title']] = subj
    
            j_graph.add((subj, rdflib.RDF.type, Journal))
            j_graph.add((subj, title, rdflib.Literal(row['title'])))
            j_graph.add((subj, languages, rdflib.Literal(row['languages'].split(','))))
            j_graph.add((subj, issn, rdflib.Literal(row['issn'])))
            j_graph.add((subj, eissn, rdflib.Literal(row['eissn'])))
            j_graph.add((subj, publisher, rdflib.Literal(row['publisher'])))
            j_graph.add((subj, seal, rdflib.Literal(row['seal'])))    
            j_graph.add((subj, license, rdflib.Literal(row['license'])))
            j_graph.add((subj, apc, rdflib.Literal(row['apc']))) 
            # THIS PART IS ADDED AFTERWARDS --   
            j_graph.add((subj,rdflib.RDF.type, Category)) 
            j_graph.add((subj, rdflib.RDF.type, Area))
            j_graph.add((subj, hasCategory, Category)) # category_int_id[row['categories']]))  # HERE I'm technically missing just the @quartile
            j_graph.add((subj, hasArea, Area)) # area_int_id[row['area']]))
            # -- THIS PART IS ADDED AFTWERWARDS. 
        # print(len(j_graph))
        return j_graph


class CategoryUploadHandler(UploadHandler): 
    def _json_file_to_df(self, json_file: str) -> pd.DataFrame: 
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            json_df = pd.DataFrame(data) 

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

# ! NOTE: TABLE NAME IS 'Category' NOT 'Categories'
# QUERY HANDLER 
class QueryHandler(Handler): 
    def __init__(self):
        super().__init__()

    def getById(self, _: str) -> pd.DataFrame: # Ila, it should return a pd.DataFrame. CHECK
        if self.getCategoriesById(id):
            return (self.getCategoriesById(id), Category)
        elif self.getAreasById(id):
            return (self.getAreasById(id), Area)
        elif self.getJournalsById(id):
            return (self.JournalsById(id), Journal)
        else:
            return pd.DataFrame, None 
        # to define later, separately
        # Both specific ID methods should be called here, returning a dataframe with all identifiable entities
class CategoryQueryHandler(QueryHandler):
    def getCategoriesById(self, id: str) -> pd.DataFrame: # Ila
        path= self.dbPathOrUrl
        try:
            with sqlite3.connect(path) as con:
                query = """
                    SELECT * FROM Category
                        OR LOWER([category]) = LOWER(?) 
                        OR LOWER([quartile]) = LOWER(?)
                """  
                params = (f"%{id.lower()}%", id.lower()) # '?' are placeholders, % are wildcards, before and after it may contain other characters 
                cat_df = pd.read_sql(query, con, params=params).drop_duplicates()
                return cat_df 
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame

    def getAreasById(self, id:str) -> pd.DataFrame: #Ila
        path= self.dbPathOrUrl
        try:
            with sqlite3.connect(path) as con:
                query = """
                    SELECT * FROM Category
                        OR LOWER([area]) = LOWER(?)
                """  
                params = (f"%{id.lower()}%") # '?' are placeholders, % are wildcards, before and after it may contain other characters 
                cat_df = pd.read_sql(query, con, params=params).drop_duplicates()
                return cat_df 
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame       

    def getAllCategories(self) -> pd.DataFrame: # Rumana
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con: 
                query = "SELECT DISTINCT category FROM Category;"
                df = pd.read_sql(query, con) # this sends the query to the dbase and stores the results in df
                return df
        except Exception as e:
            print(f"Connection to SQL database failed due to error: {e}") 
            return pd.DataFrame
            
    def getAllAreas(self) -> pd.DataFrame: # Martina
        # SELECT area FROM categories; This is the query in itself. 
        # TO BE MODIFIED in order to not have repetitions (so only the first instance is restituted).
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                q2 = "SELECT DISTINCT area FROM Category;" # DISTINCT allows to avoid showing duplicates.
                q2_df = pd.read_sql(q2, con)
                return q2_df
        except Exception as e:
                print(f"Connection to SQL database failed due to error: {e}") 
                return pd.DataFrame # in order to always return a DataFrame object, even if the queries fails for some reason.   

    def getCategoriesWithQuartile(self, quartiles: set[str]={"Q1", "Q2", "Q3", "Q4"}) -> pd.DataFrame: # ! Nico is finished this method, requires testing
        path = self.getDbPathOrUrl() # a safer way to access the path than directly accessing the variable
        categories = [] # an addition: the default argument assumes all quartiles
        query = """
            SELECT quartile, category
            FROM Categories
            WHERE quartile = ?
            ;
        """
        try:
            with sqlite3.connect(path) as con:
                for quartile in quartiles: # ? Testing one quartile at a time, addresses the blank case
                    quartile_df = pd.read_sql(query, con, params=(quartile,)) 
                    print(quartile)
        except Exception as e:
            print(f"Error in the query: {e}") 
            return pd.DataFrame  
        else:
            categories.append(quartile_df)
            all_categories = pd.concat(categories, ignore_index=True).drop_duplicates()
            return all_categories

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame: # ? Ila, it works T.T
        path = self.dbPathOrUrl
        try:
            with sqlite3.connect(path) as con:
                if area_ids:
                    area_ids_lower = [a.lower() for a in area_ids]
                    query = f"""
                        SELECT DISTINCT area, category
                        FROM Category
                        WHERE {" OR ".join(["LOWER(area) LIKE ?" for _ in area_ids_lower])}
                    """
                    df = pd.read_sql(query, con, params=[f"%{a}%" for a in area_ids_lower]).drop_duplicates() # prof doesn't want duplicates
                else:
                    query = """
                        SELECT DISTINCT area, category
                        FROM Category
                    """
                    df = pd.read_sql(query, con).drop_duplicates() # prof doesn't want duplicates 
                return df
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame
    
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame: # ? Nico is doing this one now...
        path = self.getDbPathOrUrl()
        query = """
            SELECT DISTINCT area, category
            FROM Category
        """
        try:
            with sqlite3.connect(path) as con:
                if category_ids:
                    category_ids = [f"%{category_id.lower()}%" for category_id in category_ids]
                    query += f"""WHERE {" OR ".join(["LOWER(category) LIKE ?" for _ in category_ids])}"""
                    areas_df = pd.read_sql(query, con, params=category_ids)
                else:
                    areas_df = pd.read_sql(query, con)
                areas_df = areas_df.drop_duplicates() 
                return areas_df
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame

class JournalQueryHandler(QueryHandler): # all methods return a DataFrame
    def __init__(self):
        super().__init__()

    def getJournalsById(self, id: str) -> pd.DataFrame: # ? Nico has done this one
        endpoint = self.getDbPathOrUrl()
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
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

            FILTER CONTAINS(LCASE(STR(?o)), LCASE("{id}"))
        }}
        """
        try:
            titles_df = sparql_dataframe.get(endpoint, query, True)
            return titles_df
        except Exception as e:
            print(f"Error in the SPARQL query: {e}")
            return pd.DataFrame

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
            return pd.DataFrame
        
    def getJournalsWithTitle(self, partialTitle: str): # ! Nico: Test the method! Blazegraph continues to fail...
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?internalId ?title ?issn ?eissn ?languages ?publisher ?seal ?license ?apc
        WHERE {{ # ? using the standard parameters required
            ?internalId rdf:type schema:Periodical .
            ?internalId schema:title ?title .
            ?internalId schema:issn ?issn .
            ?internalId schema:eissn ?eissn .
            ?internalId schema:inLanguage ?languages .
            ?internalId schema:publisher ?publisher .
            ?internalId schema:license ?license .
            ?internalId schema:isAccessibleForFree ?apc .
            ?internalId schema:doajSeal ?seal .

            FILTER CONTAINS(LCASE(STR(?title)), LCASE("{partialTitle}")) # ? matching the title and the partial title
        }}
        """
        try:
            titles_df = sparql_dataframe.get(self.dbPathOrUrl, query, True)
            return titles_df
        except Exception as e:
            print(f"Error in the SPARQL query: {e}")
            return pd.DataFrame

    def getJournalsPublishedBy(self, partialName: str): # ? Ila : it works
        endpoint = self.getDbPathOrUrl()        
        query = f"""
        PREFIX res:    <https://github.com/git-lost-data-science/res/>
        PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?title ?issn ?eissn ?publisher ?languages ?seal ?license ?apc
        WHERE {{
            ?s rdf:type schema:Periodical .
            ?s schema:publisher ?o .
            ?s schema:name ?title .
            ?s schema:issn ?issn .
            ?s schema:eissn ?eissn .
            ?s schema:publisher ?publisher .
            ?s schema:inLanguage ?languages .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:isAccessibleForFree ?apc .
            FILTER CONTAINS(LCASE(STR(?o)), LCASE("{partialName}"))
        }}
        """
        try:
            df = sparql_dataframe.get(endpoint, query, True)
            return pd.DataFrame(df)
        except Exception as e:
            print(f"Error in SPARQL query: {e}")
            return pd.DataFrame
            
    def getJournalsWithLicense(self, licenses: set[str]): # Rumana
        if not licenses:
            return pd.DataFrame
        try:
            endpoint = self.getDbPathOrUrl()
                
            licenses_filter = " || ".join([f'?license = "{license}"' for license in licenses])
            query = f'''
            PREFIX schema: <https://schema.org/>
            SELECT ?journal ?title ?license
            WHERE {{
                ?journal rdf:type schema:Periodical .
                ?journal schema:name ?title .
                ?journal schema:license ?license .
                FILTER ({licenses_filter})
            }}
            '''
            journal_df = sparql_dataframe.get(endpoint, query, True)
            return journal_df
    
        except Exception as e:
                print(f"Connection to SPARQL endpoint failed due to error: {e}") 
                return pd.DataFrame
            
    def getJournalsWithAPC(self): # Martina
        try:
            endpoint = self.getDbPathOrUrl()
            jouAPC_query = '''
            SELECT ?title ?issn ?eissn ?publisher ?languages ?seal ?license ?apc
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s schema:name ?title .
                ?s schema:issn ?issn .
                ?s schema:eissn ?eissn .
                ?s schema:publisher ?publisher .
                ?s schema:inLanguage ?languages .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:isAccessibleForFree ?apc .
                FILTER (?apc = true)
            }
            ''' 
            # After a million times of loading the data the query works correctly filtering ONLY the Journals with APC (apc = true)
            # if this was actually what was required...
            
            jouAPC_df = sparql_dataframe.get(endpoint, jouAPC_query, True)
            return jouAPC_df
        
        except Exception as e:
            print(f"Error in SPARQL query due to: {e}") 
            return pd.DataFrame
    
    def JournalsWithDOAJSeal(self): # ? Nico, done, untested
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?title ?seal
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s schema:name ?title .
                ?s schema:hasDOAJSeal ?seal . 
            }
            """
            
            journal_DOAJ_df = sparql_dataframe.get(endpoint, query, True)
            return journal_DOAJ_df
        
        except Exception as e:
            print(f"The query was unsuccessful due to the following error: {e}") 
            return pd.DataFrame
        
class BasicQueryEngine:
    def __init__(self, journalQuery, categoryQuery): # ? Ila, done
        self.journalQuery= []
        self.categoryQuery= []

    def cleanJournalHandlers(self) -> bool:
        self.journalQuery= []
        return True
    def cleanCategoryHandlers(self) -> bool: #  ? Ila, done
        self.categoryQuery= []
        return True       
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool: # ? Martina, done
        try:
            self.journalQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to: {e}")
            return False # appends the journal handler to the journal handlers
            
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool: # ? Nico, done
        try:
            self.categoryQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the category handlers to the categoryQuery

    def getEntityById(entity: str) -> Optional[IdentifiableEntity]: # Rumana
        # ensure consistency of return values! this may need to be transformed into a tuple
        entity_df, idType = entity # entity_df is a dataframe, idType is the type of the object
        if isinstance(idType, Category):
            for _, row in entity_df.iterrows(): # what if more than one value exists? Nico is concerned
                return Category(row.get("category"), row.get("quartile"))
            # transform the object into a Category
        elif isinstance(idType, Area):
            for _, row in entity_df.iterrows():
                return Area(row.get("area"))
        elif isinstance(idType, Journal):
            for _, row in entity_df.iterrows():
                return Journal(row.get("issn"), row.get("title"), row.get("languages"), row.get("publisher"), 
                               row.get("seal"), row.get("licence"), row.get("apc"), None, None)
        else:
            return None

    def getAllJournals(self) -> list[Journal]: # ! Ila, to be tested
        all_data = []

        for journal in self.journalQuery:  # it looks at all the queries in the list journalQuery
            if isinstance(journal, JournalQueryHandler):  # checks if it is an instance of the JournalQueryHandler
                df = journal.getAllJournals()  # calls the previous method on it
                all_data.append(df) # appends the result of that method in a list
        if all_data:
            db = pd.concat(all_data, ignore_index=True).drop_duplicates() # we can have more than one df, so let's drop duplicates 
            db = db[['id', 'title', 'issn', 'eissn', 'publisher', 'languages', 'license', 'apc', 'seal']].fillna('') # take these rows from the cleared db 
            result = [
                Journal(
                    row['id'],  # the internal id of the Journal ???????? is it there????
                    row['title'],
                    row['issn'],
                    row['eissn'],
                    row['publisher'],
                    row['languages'],
                    row['license'],
                    row['apc'],
                    row['seal']
                ) for _, row in db.iterrows()
            ]
            return result
        else:
            return []

    def getJournalsWithTitle(self, partialTitle:str) ->list[Journal]: # Martina
        all_jou = []
        result = []

        for handler in self.journalQuery: 
            if isinstance(handler, JournalQueryHandler):
                df = handler.getJournalsWithTitle()
                all_jou.append(df)

        if all_jou:
            db = pd.concat(all_jou, ignore_index=True).drop_duplicates()
            db = db[['id', 'title', 'publisher', 'languages', 'license', 'apc', 'seal']].fillna('')
            
            substring = partialTitle.lower()
            match = db['title'].astype(str).str.lower().str.contains(substring, na=False) 
            matching_db = db[match]

            for idx, row in matching_db.iterrows():
                jou_obj = Journal(
                    id = row.get('id'),
                    title = row.get('title'),
                    # issn = row.get('issn'),
                    # eissn = row.get('eissn'),
                    publisher = row.get('publisher'),
                    languages = row.get('languages').split(','),
                    license = row.get('license'),
                    apc = row.get('apc'),
                    seal = row.get('seal')
                ) 
                # the only worry I have is that ISSN and EISSN will create a conlfict because they're not part of the defined attributes in __init__ of Journal
                # to fix this I think we can just delete the selected columns of 'issn' and 'eissn in db
                result.append(jou_obj)
            return result
        else:
            return []
        
    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]: # ! Nico, requires revisions in line with modifications
        journals = [journal.getJournalsPublishedBy(partialName) for journal in self.journalQuery]
        all_journals = []

        if journals:
            journals = pd.concat(journals, ignore_index=True).drop_duplicates().fillna("")  
            for _, row in journals.iterrows(): # none is used twice as a placeholder...
                journal = Journal(row.get("eissn"), row.get("title"), row.get("languages"), row.get("publisher"), 
                                  row.get("seal"), row.get("licence"), row.get("apc"), None, None) 
                all_journals.append(journal) # maybe the eissn is the internal id for the journal (I'm not sure?)
                
        return all_journals # 'issn', 'title', 'languages', 'publisher', 'seal', 'licence', 'apc'

    def getJournalsWithLicense(self, licenses:set[str]) -> list[Journal]: # Rumana
        try:
            all_journal_dfs = []

            for handler in self.journalQuery:
                if isinstance(handler, JournalQueryHandler):
                    df = handler.getJournalsWithLicense(licenses)
                    if not df.empty:
                        all_journal_dfs.append(df)

            if not all_journal_dfs:
                return []

            db = pd.concat(all_journal_dfs, ignore_index=True).drop_duplicates()
            db = db[['id', 'title', 'publisher', 'languages', 'license', 'apc', 'seal']].fillna('')
            # Clean the license column
            db['license'] = db['license'].str.strip()
            db = db[db['license'].isin(licenses)]

            result = []
            for _, row in db.iterrows():
                journal = Journal(
                    id=row.get('id'),
                    title=row.get('title'),
                    publisher=row.get('publisher'),
                    languages=row.get('languages').split(',') if row.get('languages') else [],
                    license=row.get('license'),
                    apc=row.get('apc'),
                    seal=row.get('seal')
                )
                result.append(journal)

            return result

        except Exception as e:
            print(f"Error while getting journals by license: {e}")
            return []
            
    def getJournalsWithAPC(self) -> list[Journal]: # Ila 
        all_data = []
        for journal in self.journalQuery:  # it looks at all the queries in the list journalQuery
            if isinstance(journal, JournalQueryHandler):  # checks if it is an instance of the JournalQueryHandler
                df = journal.getJournalsWithAPC()  # calls the previous method on it
                all_data.append(df) # appends the result of that method in a list
        if all_data:
            db = pd.concat(all_data, ignore_index=True).drop_duplicates() # we can have more than one df, so let's drop duplicates 
            db = db[['title', 'issn', 'eissn', 'publisher', 'languages', 'seal', 'license', 'apc']].fillna('') # take these rows from the cleared db 
            result = [
                Journal(
                    row.get('title'),
                    row.get('issn'),
                    row.get('eissn'),
                    row.get('publisher'),
                    row.get('languages'),
                    row.get('seal'),
                    row.get('license'),
                    row.get('apc')
                ) for _, row in db.iterrows()
            ]
            return result
        else:
            return [] 
            
    def getJournalsWithDOAJSeal(self) -> list[Journal]: # Martina
        jou_seal = []
        res_obj = []

        for handler in self.journalQuery: 
            if isinstance(handler, JournalQueryHandler):
                df = handler.getJournalsWithDOAJSeal()
                jou_seal.append(df)

        if jou_seal:
            db = pd.concat(jou_seal, ignore_index=True).drop_duplicates()
            db = db[['id', 'title', 'publisher', 'languages', 'license', 'apc', 'seal']].fillna('') 
            db['seal'] = db['seal'].astype(bool) # ensuring it is treated as a boolean type if anything happens
            
            for idx, row in db.iterrows():
                if db.query('seal == True'): #db['seal'] == True: maybe using the query is more efficient
                    jou_obj = Journal(
                        id = row.get('id'),
                        title = row.get('title'),
                        # issn = row.get('issn'),
                        # eissn = row.get('eissn'),
                        publisher = row.get('publisher'),
                        languages = row.get('languages').split(','),
                        license = row.get('issn'),
                        apc = row.get('apc'),
                        seal = row.get('seal')
                    ) 
                    res_obj.append(jou_obj) 
            return res_obj
        else:
            return []

    def getAllCategories(self) -> list[Category]: # ! Nico, working but does not address the problem of duplicates and multiple quartiles
        # The getAllCategories method had to be modified to include quartiles...
        category_dfs = [category.getAllCategories() for category in self.categoryQuery] # list comprehension to generate
        all_categories = []
    
        if category_dfs:
            category_dfs = pd.concat(category_dfs, ignore_index=True).drop_duplicates()  
            pprint(category_dfs)
            # no need to fill blank values...
            for _, row in category_dfs.iterrows():
                category = Category(row.get("category"), row.get("quartile"))
                all_categories.append(category)
            # steps: first get categories, merge categories if they are mentioned multiple times with different quartiles...
        return all_categories
    
    def getAllAreas(self) -> list[Area]: # Rumana
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                query = "SELECT DISTINCT area FROM categories;"  # Query to fetch unique areas
                df = pd.read_sql(query, con)  # Execute the query and store results in a DataFrame
                
                areas = []
                for _, row in df.iterrows():
                    area = Area(id=row['area'])  # Create Area objects for each unique area
                    areas.append(area)
                
                return areas
        except Exception as e:
            print(f"Error fetching areas: {e}")
            return []
            
    def getCategoriesWithQuartile(self, quartiles:set[str]) -> list[Category]: # Ila
        all_data = []
        for category in self.categoryQuery:  # it looks at all the queries in the list journalQuery
            if isinstance(category, CategoryQueryHandler):  # checks if it is an instance of the JournalQueryHandler
                df = category.getCategoriesWithQuartile()  # calls the previous method on it
                all_data.append(df) # appends the result of that method in a list
        if all_data:
            db = pd.concat(all_data, ignore_index=True).drop_duplicates() # we can have more than one df, so let's drop duplicates 
            db = db[['quartile', 'category']].fillna('') # take these rows from the cleared db 
            result = [Category(row.get('quartile'),row.get('category')) for _, row in db.iterrows()]
            return result
        else:
            return []
        
    def getCategoriesAssignedToAreas(self, areas_ids: set[str]) -> list[Category]: # Martina
        cat_areas = []
        assigned_cat = []

        for handler in self.categoryQuery: 
            if isinstance(handler, CategoryQueryHandler):
                df = handler.getCategoriesAssignedToAreas()
                cat_areas.append(df)
        
        if cat_areas:
            db = pd.concat(cat_areas, ignore_index=True).drop_duplicates()
            db = db[['internal-id', 'journal-ids', 'category', 'quartile', 'area']].fillna('')

            #areas_ids = areas_ids.astype(str).split(',')
            
            area = ','.join(str(area) for area in areas_ids)
            match = db['area'].astype(str).str.lower().str.contains(area, na=False) 
            matching_db = db[match]

            for idx, row in matching_db.iterrows():
                if cat_obj not in assigned_cat:  # technically avoiding repetitions in the list returned (?) NEEDS TO BE FIXED
                    cat_obj = Category(
                        id = row.get('id'),
                        quartile = row.get('quartile')
                    )
                    assigned_cat.append(cat_obj)
            return assigned_cat
        else:
            return []
            
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]: # * Nico, done and working 
        assigned_areas = [] # methods like these should be responsible for all the filtering done

        categories = [category.getAreasAssignedToCategories(category_ids) for category in self.categoryQuery]

        if categories:
            categories = pd.concat(categories, ignore_index=True).drop_duplicates().fillna("")
 
            category_match = {str(category_id).lower() for category_id in category_ids}
            categories_match = categories["category"].astype(str).str.lower().isin(category_match)
            categories = categories[categories_match]

            for _, row in categories.iterrows():
                area = Area(row.get("area"))
                if area not in assigned_areas: # working thanks to the definition of equality in the IdentifiableEntity class
                    assigned_areas.append(area)

        return assigned_areas

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]): # Nico
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str]): # Ila 
        jou= pd.DataFrame()
        cat= pd.DataFrame()
        result = []
        # method getJournalsWithLicense, getCategoriesAssignedToAreas
        # takes the df of both of the methods (one returns a list of Journal obj, another a list of Category obj)
        # change the name of the column I am interested in (issn and journal-ids in this case)
        # merges the results with pd.merge(1_df, 2_df, left on= " ", right_on= "") 
        # appends all the journals in the result list 
        for journal in self.journalQuery:
            if isinstance(journal, JournalQueryHandler):
                all_jou = journal.getJournalsWithLicense()
                jou = pd.concat([jou, all_jou], ignore_index=True)

        for category in self.categoryQuery:
            if isinstance(category, CategoryQueryHandler):
                all_cat = category.getCategoriesAssignedToAreas(areas_ids)
                cat = pd.concat([cat, all_cat], ignore_index=True)

        # Extract only the first id of the journal-ids wor, since the category has both the issn an eissn values in one column 
        cat['journal_ids'] = cat['journal_ids'].apply(lambda x: x.split(',')[0] if isinstance(x, str) else x)
        merged = pd.merge(jou, cat, left_on='issn', right_on='journal_ids').drop_duplicates()

        for _, row in merged.iterrows():
            result.append(Journal(
                row.get('id'),
                row.get('title'),
                row.get('issn'),
                row.get('eissn'),
                row.get('publisher'),
                row.get('languages'),
                row.get('license'),
                row.get('apc'),
                row.get('seal')
            ))
        return result  
    
    def getDiamondJournalsAreasAndCAtegoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]): # Martina
        pass
