import json
import os
import re
from inspect import currentframe
from typing import Optional, Self

import numpy
import pandas as pd
import rdflib
import sqlite3 
import sparql_dataframe 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


class TypeMismatchError(Exception):
    def __init__(self, expected_type_description: str, obj: object):
        actual_type_name = type(obj).__name__
        preposition = "an" if actual_type_name[0] in "aeiou" else "a"
        super().__init__(f"Expected {expected_type_description}, got {preposition} {actual_type_name}.")

class IdentifiableEntity:
    def __init__(self, id: list[str] | str): 
        if not (isinstance(id, list) and all(isinstance(value, str) for value in id)) and not isinstance(id, str):
            raise TypeMismatchError("a list of strings or a string", id)
        self.id: list[str] | str = id 

    def __eq__(self, other: Self) -> bool: # checking for value equality
        return self.id == other.id 
    
    def getIds(self) -> list[str]:
        if isinstance(self.id, str):  # M: changed this so it handles the fact that id can also be a string 
            return [self.id]
        return self.id
    
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

class Journal(IdentifiableEntity):
    def __init__(self, id, title: str, languages: str | list, publisher: Optional[str], 
                 seal: bool, license: str, apc: bool):
        # ?? hasCategory: Optional[list[Category]], hasArea: Optional[list[Area]]): are included in the list
        super().__init__(id)
        
        # if not (isinstance(title, str) or title):

        if not isinstance(title, str) and not title:
            raise TypeMismatchError("a non-empty str", title) # ? the same as raise TypeError(f"Expected a non-empty str, got {type(title).__name__}")
        
        if not isinstance(languages, (str, list)): # ! Ila changed this
            raise TypeMismatchError("a non-empty str or list", languages)
        if isinstance(languages, list) and not all(isinstance(lang, str) for lang in languages):
            raise TypeMismatchError("a list of strings", languages)
        if not languages: # handles empty string or empty list
            raise TypeMismatchError("a non-empty str or list", languages)
                
        if not isinstance(publisher, str) and not publisher:
            raise TypeMismatchError("a str or a NoneType", publisher)
        
        if not isinstance(seal, bool) and not isinstance(seal, numpy.bool):
            raise TypeMismatchError("a boolean", seal)
        
        if not isinstance(license, str) and not license:
            raise TypeMismatchError("a non-empty str", license)
        
        if not isinstance(apc, bool) and not isinstance(apc, numpy.bool):
            raise TypeMismatchError("a boolean", apc)
        
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.hasCategory: list[Category] = [] 
        self.hasArea: list[Area] = []

    def addCategory(self, category): 
        if not isinstance(category, Category):
            raise ValueError("category must be an instance of Category.")
        self.hasCategory.append(category)

    def addArea(self, area): # same here
        if not isinstance(area, Area):
            raise ValueError("area must be an instance of Area.")
        self.hasArea.append(area)

    def getTitle(self):
        return self.title

    def getLanguages(self):
        if isinstance(self.languages, str):
            return [self.languages]
        return self.languages

    def getPublisher(self):
        return self.publisher

    def hasDOAJSeal(self):
        return self.seal

    def getLicense(self):
        return self.license

    def hasAPC(self):
        return self.apc

    def getCategories(self):
        return self.hasCategory

    def getAreas(self):
        return self.hasArea

class Handler: 
    def __init__(self):
        self.dbPathOrUrl = "" # When initialised, there is no set database path or URL

    def getDbPathOrUrl(self): # @property 
        return self.dbPathOrUrl 
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:  # setter
        if not pathOrUrl or not pathOrUrl.strip(): 
            return False
        if pathOrUrl.endswith(".db") or "blazegraph" in pathOrUrl: 
            self.dbPathOrUrl = pathOrUrl 
            return True
        return False 

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, _: str) -> bool: 
        pass

class JournalUploadHandler(UploadHandler):
    def createJournalGraph(self, csv_file: str) -> rdflib.Graph: # Martina
        j_graph = rdflib.Graph() # initialising an empty graph
            
        # referencing all the classes:    
        Journal = rdflib.URIRef("https://schema.org/Periodical")   
    
        # referencing the attributes:
        id = rdflib.URIRef("https://schema.org/identifier")
        title = rdflib.URIRef("https://schema.org/name")
        languages = rdflib.URIRef("https://schema.org/inLanguage") # (superseded /Language)
        publisher = rdflib.URIRef("https://schema.org/publisher")
        seal = rdflib.URIRef("https://schema.org/hasDOAJSeal") # invented
        license = rdflib.URIRef("https://schema.org/license")
        apc = rdflib.URIRef("https://schema.org/hasAPC") # this is a boolean value in theory so it should work.
            
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
    
            journals_int_id[row["title"]] = subj

            issn = str(row.get("issn", "")).strip() # Putting this code in here to reduce redundant for loops
            eissn = str(row.get("eissn", "")).strip()

            if issn and eissn:
                combined_ids = f"{issn}, {eissn}"
            elif issn and not eissn:
                combined_ids = issn
            elif eissn and not issn:
                combined_ids = eissn
            else:
                combined_ids = ""
            
            # current_languages_str = str(row.get("languages", "")).strip() 
            # if current_languages_str: 
            #     list_of_languages = [lang.strip() for lang in current_languages_str.split(",")]
            #     for lang_item in list_of_languages:
            #         if lang_item: 
            #             j_graph.add((subj, languages, rdflib.Literal(lang_item)))
        
            j_graph.add((subj, rdflib.RDF.type, Journal))
            j_graph.add((subj, id, rdflib.Literal(combined_ids)))
            j_graph.add((subj, title, rdflib.Literal(row["title"])))
            j_graph.add((subj, languages, rdflib.Literal(row["languages"])))
            j_graph.add((subj, publisher, rdflib.Literal(row["publisher"])))
            j_graph.add((subj, seal, rdflib.Literal(row["seal"])))    
            j_graph.add((subj, license, rdflib.Literal(row["license"])))
            j_graph.add((subj, apc, rdflib.Literal(row["apc"]))) 
        return j_graph
    
    def pushDataToDb(self, path: str) -> bool: 
        jou_graph = self.createJournalGraph(path)
        try:
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl()
            store.open((endpoint, endpoint))
            for triple in jou_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            return True
        except Exception as e:
            print(f"Error during pushDataToDb (CSV to Blazegraph): {e}")
            return False

class CategoryUploadHandler(UploadHandler): 
    def createCategoryDataframe(self, json_file: str) -> pd.DataFrame: # Ila
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
                            "quartile": cat.get("quartile", None), 
                            "area": area
                        })

            categories_df = pd.DataFrame(rows)
            return categories_df 
    
    def pushDataToDb(self, path: str) -> bool: 
        categories_df = self.createCategoryDataframe(path)

        absolute_path = os.path.abspath(path) # Ila: os uses always an absolute path
        if not os.path.exists(absolute_path):
            return False
        
        try:
            with sqlite3.connect(self.dbPathOrUrl) as con:
                categories_df.to_sql("Category", con, if_exists="replace", index=False)
                con.commit()
            return True
        except sqlite3.Error as e: # Gemini's suggestion to catch specific errors
            print(f"SQLite error during pushDataToDb (JSON): {e}")
            return False
        except Exception as e: 
            print(f"Unexpected error during pushDataToDb (JSON): {e}")
            return False

class QueryHandler(Handler): 
    def __init__(self):
        super().__init__()
    
    @property
    def queryType(self) -> str: # used for handling exceptions of different types
        pass

    def getById(self, _: str) -> pd.DataFrame: # * Nico, working
        pass
    
    def unexpectedDatabaseError(self, e: Exception): # added for standardisation and better error diagnosis
        stack_frame = currentframe().f_back
        function_name = stack_frame.f_code.co_name
        print(f"Unexpected error during {function_name!r} ({self.queryType}) [{type(e).__name__}]: {e}")

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    @property
    def queryType(self) -> str:
        return "SQLite"
    
    def getById(self, id: str) -> pd.DataFrame: # * working
        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(, \d{4}-\d{3,4}X?)*$')

        if not journal_id_pattern.match(id): 
            entity_types = ["category", "area", "journal-ids"] # journal-ids last...
            for entity_type in entity_types:
                object_df = self.getCategoryObjectsById(id, entity_type)
                if not object_df.empty:
                    return self.createCategoryObject(object_df, entity_type) 
            
        else: # for matching journal ids to their categories and quartiles
            possible_journal_ids = [id] + id.split(", ") # Ila: fixed the syntax
            for journal_id in possible_journal_ids:
                journal_ids_df = self.getCategoryObjectsById(journal_id, "journal-ids")
                if not journal_ids_df.empty:
                    return self.createCategoryObject(journal_ids_df, "journal")
    
        return pd.DataFrame()
    
    def createCategoryObject(self, target_df: pd.DataFrame, entity_type: str) -> pd.Series:  
        if entity_type == "journal":
            categories_with_quartiles = {}
            areas = set()
            for _, row in target_df.iterrows():
                categories_with_quartiles[row["category"]] = row.get("quartile")
                areas.add(row.get("area"))

            journal_category_values = [target_df.iloc[0]["journal-ids"], categories_with_quartiles, areas]
            journal_category_data = pd.DataFrame([journal_category_values], columns=["journal-ids", "categories-with-quartiles", "areas"])
            return journal_category_data

        elif entity_type == "area":
            areas = list(set(row.get("area") for _, row in target_df.iterrows()))
            target_area = pd.DataFrame([areas[0]], columns=["area"])
            return target_area

        elif entity_type == "category":
            categories = list(set(row.get("category") for _, row in target_df.iterrows())) # sets to prevent duplicates
            unique_quartiles = list(set(row.get("quartile") for _, row in target_df.iterrows() if row.get("quartile") is not None))

            if not unique_quartiles:
                quartiles = None
            elif len(unique_quartiles) == 1:
                quartiles = unique_quartiles[0]
            elif len(unique_quartiles) < 4: 
                quartiles = ", ".join(sorted(unique_quartiles)) 
            else: 
                quartiles = None
            
            target_category = pd.DataFrame([[categories[0], quartiles]], columns=["category", "quartile"])
            return target_category

        else:
            return pd.DataFrame()

    def getCategoryObjectsById(self, id: str, entity_type: str) -> pd.DataFrame: 
        path = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(path) as con:
                query = f"""
                    SELECT DISTINCT *
                    FROM Category
                    WHERE LOWER("{entity_type}") = LOWER(?);
                """ 
                params = id.lower() 
                cat_df = pd.read_sql(query, con, params=(params,)).drop_duplicates()
                return cat_df 
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getAllCategories(self) -> pd.DataFrame: # * Rumana, working
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                query = "SELECT DISTINCT category FROM Category;"
                df = pd.read_sql(query, con)
                return df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()
            
    def getAllAreas(self) -> pd.DataFrame: # * Martina, working
        path = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(path) as con:
                query = "SELECT DISTINCT area FROM Category;" # DISTINCT allows to avoid showing duplicates.
                areas_df = pd.read_sql(query, con)
                return areas_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame() # in order to always return a DataFrame object, even if the queries fails for some reason.   

    def getCategoriesWithQuartile(self, quartiles: Optional[set[str]]) -> pd.DataFrame: # * Nico, working
        path = self.getDbPathOrUrl() # a safer way to access the path than directly accessing the variable
        categories_with_quartiles_df = pd.DataFrame([], columns=["category", "quartile"])

        try:
            with sqlite3.connect(path) as con:
                if not quartiles:
                    query = """
                    SELECT DISTINCT category
                    FROM Category;
                    """
                    category_df = pd.read_sql(query, con).iloc[::-1]
                    
                else:
                    query = f"""
                    SELECT DISTINCT category
                    FROM Category 
                    WHERE {" OR ".join(["quartile = ?" for _ in quartiles])}
                    """
                    category_df = pd.read_sql(query, con,  params=[f"{quartile}" for quartile in quartiles]).iloc[::-1]
            
            for _, row in category_df.iterrows():
                category_id = row.get("category")
                new_category = self.getById(category_id)
                categories_with_quartiles_df = pd.concat([new_category, categories_with_quartiles_df], ignore_index=True)

            return categories_with_quartiles_df
        
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()  

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame: # * Ila, working
        path = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(path) as con:
                if area_ids:
                    area_ids_lower = [a.lower() for a in area_ids]
                    query = f"""
                        SELECT DISTINCT area, category
                        FROM Category
                        WHERE {" OR ".join(["LOWER(area) LIKE ?" for _ in area_ids_lower])}
                    """
                    df = pd.read_sql(query, con, params=[f"{a}" for a in area_ids_lower])
                else:
                    query = """
                        SELECT DISTINCT area, category
                        FROM Category
                    """
                    df = pd.read_sql(query, con)
                return df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()
    
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame: # * Nico, working
        path = self.getDbPathOrUrl()
        query = """
            SELECT DISTINCT area, category
            FROM Category
        """
        try:
            with sqlite3.connect(path) as con:
                if category_ids:
                    category_ids = [f"{category_id.lower()}" for category_id in category_ids]
                    query += f"""WHERE {" OR ".join(["LOWER(category) LIKE ?" for _ in category_ids])}"""
                    areas_df = pd.read_sql(query, con, params=category_ids)
                else:
                    areas_df = pd.read_sql(query, con)
                areas_df = areas_df.drop_duplicates() 
                return areas_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    @property
    def queryType(self) -> str:
        return "Blazegraph"

    def getById(self, id: str) -> pd.DataFrame: # * working
        possible_journal_ids = id.split(", ")
        possible_journal_ids.insert(0, id) # adding this possibility too (i.e. all ids are together)
        for possible_journal_id in possible_journal_ids:
            journals_df = self.getJournalById(possible_journal_id) # only for this!
            if not journals_df.empty:
                break
        else:
            return pd.DataFrame()

        journal_df = journals_df.iloc[0]
        journal_row_values = [journal_df["journal-ids"], journal_df["title"], journal_df["languages"], journal_df["publisher"], 
                                journal_df["seal"], journal_df["license"], journal_df["apc"]]
        journal_df = pd.DataFrame([journal_row_values], columns=["journal-ids", "title", "languages", "publisher", "seal", "license", "apc"])
        return journal_df

    def getJournalById(self, id: str) -> pd.DataFrame: 
        endpoint = self.getDbPathOrUrl()
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
        
            FILTER CONTAINS(LCASE(STR(?id)), LCASE("{id}"))
        }} 
        """
        try:
            titles_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            return titles_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getAllJournals(self): # * Martina, working
        endpoint = self.getDbPathOrUrl()
        journal_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
        }} 
        """
        try:    
            journal_df = sparql_dataframe.get(endpoint, journal_query, True).rename(columns={"id": "journal-ids"})
            return journal_df
        
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()
        
    def getJournalsWithTitle(self, partialTitle: str): # * Nico, working
        endpoint = self.getDbPathOrUrl()
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .

            FILTER CONTAINS(LCASE(STR(?title)), LCASE("{partialTitle}"))
        }} 
        """

        try:
            titles_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            return titles_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getJournalsPublishedBy(self, partialName: str): # * Ila: works fine even with Torino
        endpoint = self.getDbPathOrUrl()
        safe_partialName = json.dumps(partialName)[1:-1] # for controlling special characters- the json method adds the quotes and [1: -1] removes them
        
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .

            FILTER CONTAINS(LCASE(STR(?publisher)), LCASE("{safe_partialName}"))
        }} 
        """
        try:
            journals_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            return journals_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getJournalsWithLicense(self, licenses: set[str]) -> pd.DataFrame: # * Rumana, it works
        endpoint = self.getDbPathOrUrl()
    
        l_set = {l.strip().lower() for l in licenses}

        filters = []
        for license_val in l_set:
            license_val_escaped = license_val.replace('"', '\\"')
            filters.append(f'LCASE(STR(?license)) = "{license_val_escaped}"')  # Exact match

        filter_clause = " || ".join(filters)

        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{
            ?s rdf:type schema:Periodical ;
            schema:identifier ?id ;
            schema:name ?title ;
            schema:inLanguage ?languages ;
            schema:publisher ?publisher ;
            schema:hasDOAJSeal ?seal ;
            schema:license ?license ;
            schema:hasAPC ?apc .
            FILTER ({filter_clause})
        }}
        """
        try:
            jou_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            return jou_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getJournalsWithAPC(self): # * Martina, working
        endpoint = self.getDbPathOrUrl()
        jouAPC_query = """
        PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
            
            FILTER (?apc = true)
        }}
        """
        try:
            jouAPC_df = sparql_dataframe.get(endpoint, jouAPC_query, True).rename(columns={"id": "journal-ids"})
            return jouAPC_df
        
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self): # * Nico, working
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
            
            FILTER (?seal = true)
        }
        """
        try:
            journal_DOAJ_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            return journal_DOAJ_df
        
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

class BasicQueryEngine:
    def __init__(self): # Ila, working 
        self.journalQuery = []
        self.categoryQuery = []

    def cleanJournalHandlers(self) -> bool: # Ila, done
        self.journalQuery = []
        return True
                 
    def cleanCategoryHandlers(self) -> bool: # Ila, done 
        self.categoryQuery = []
        return True  
         
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool: # * Martina, working
        try:
            self.journalQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the journal handler to the journal handlers
            
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool: # * Nico, working
        try:
            self.categoryQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the category handlers to the categoryQuery

    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]: # * Nico, working with journals, areas, and categories
        if not id: # preventing any errors from results
            return None

        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(, \d{4}-\d{3,4}X?)*$')

        if journal_id_pattern.match(id) is not None: # when it is a journal
            journal_found = False

            for journalQueryHandler in self.journalQuery:
                journal_object = journalQueryHandler.getById(id)
                if not journal_object.empty:
                    journal_found = True
                    journal_object = journal_object.iloc[0]
                    break # proceed, the object has been found...

            if not journal_found:
                return None
            
            journal = Journal( 
                journal_object["journal-ids"], 
                journal_object["title"], 
                journal_object["languages"], 
                journal_object["publisher"], 
                bool(journal_object["seal"]), 
                journal_object["license"], 
                bool(journal_object["apc"])
            )

            matching_category_data_found = False

            for categoryQueryHandler in self.categoryQuery:
                journal_category_data = categoryQueryHandler.getById(id)
                if not journal_category_data.empty:
                    matching_category_data_found = True
                    journal_category_data = journal_category_data.iloc[0]
                    break

            if matching_category_data_found: # * Change made: categories and areas are ONLY added if there is matching category data found
                categories_with_quartiles = journal_category_data["categories-with-quartiles"]
                area_values = journal_category_data["areas"]

                for category_value, quartile_value in categories_with_quartiles.items():
                    category = Category(category_value, quartile_value) # each category is now created with only ONE quartile each
                    journal.addCategory(category)

                for area_value in area_values:
                    area = Area(area_value)
                    journal.addArea(area)

            return journal

        else:
            for categoryQueryHandler in self.categoryQuery:
                category_object = categoryQueryHandler.getById(id)
                if "category" in category_object.columns and not category_object["category"].isnull().all(): 
                    category = category_object.iloc[0]
                    return Category(category["category"], category["quartile"])
                if "area" in category_object.columns and not category_object["area"].isnull().all():
                    area = category_object.iloc[0]
                    return Area(area["area"])

            return None # pushed back a line – if it's not in one category query handler, it might be in the next

    def getAllJournals(self) -> list[Journal]: # Ila, tested, working
        return self._getLimitedJournals(limit=100)  # limiting the number of the journals for testing purposes 

    def _getLimitedJournals(self, limit: int | None = None) -> list[Journal]:
        
        all_journals = []
        seen_ids = set()

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getAllJournals()
            if journals_df.empty: # it the columns is empty, ignore it, go on 
                continue

            for journal_id in journals_df["journal-ids"]:
                if journal_id in seen_ids:
                    continue # ignore the journal 

                journal = self.getEntityById(journal_id)
                if journal is not None:
                    all_journals.append(journal)
                    seen_ids.add(journal_id)

                if limit is not None and len(all_journals) >= limit: # if the limit has been reached or ignored, return the list made
                    return all_journals

        return all_journals


    # def getAllJournals(self) -> list[Journal]: # * Ila, working but slowly
    #     all_journals = [] 

    #     for journalQueryHandler in self.journalQuery:
    #         journals_df = journalQueryHandler.getAllJournals()
    #         if journals_df.empty:
    #             continue

    #         for journal_ids in journals_df["journal-ids"]: 
    #             journal = self.getEntityById(journal_ids) 
    #             if journal not in all_journals:
    #                 all_journals.append(journal) 

    #     return all_journals

    def getJournalsWithTitle(self, partialTitle: str) ->list[Journal]: # * Martina, working
        journals_with_title = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getJournalsWithTitle(partialTitle)
            if journals_df.empty:   
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal not in journals_with_title:
                    journals_with_title.append(journal)

        return journals_with_title
        
    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]: # * Nico, working
        journals_published_by = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getJournalsPublishedBy(partialName)
            if journals_df.empty:   
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal not in journals_published_by:
                    journals_published_by.append(journal)

        return journals_published_by

    def getJournalsWithLicense(self, licenses: str) -> list[Journal]: # ?? Rumana, fix the issue in the JournalQueryHandler to fix this function
        journals_with_license = []

        for journalQueryHandler in self.journalQuery: 
            journals_df = journalQueryHandler.getJournalsWithLicense(licenses) 
            if journals_df.empty:     
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal is not None and journal not in journals_with_license:
                    journals_with_license.append(journal)

        return journals_with_license
            
    def getJournalsWithAPC(self) -> list[Journal]: # * Ila, works in 20 minutes 
        journals_with_APC = []

        for journalQueryHandler in self.journalQuery: 
            journals_df = journalQueryHandler.getJournalsWithAPC() 
            if journals_df.empty:     
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal is not None and journal not in journals_with_APC:
                    journals_with_APC.append(journal)

        return journals_with_APC
            
    def getJournalsWithDOAJSeal(self) -> list[Journal]: # * Martina, working
        journals_with_DOAJ_seal = []

        for journalQueryHandler in self.journalQuery: 
            journals_df = journalQueryHandler.getJournalsWithDOAJSeal() 
            if journals_df.empty:   
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal is not None and journal not in journals_with_DOAJ_seal:
                    journals_with_DOAJ_seal.append(journal)

        return journals_with_DOAJ_seal                         

    def getAllCategories(self) -> list[Category]: # * Nico, working
        all_categories = []

        for categoryQueryHandler in self.categoryQuery:
            categories_df = categoryQueryHandler.getAllCategories()
            if categories_df.empty:
                continue

            for category_id in categories_df["category"]:
                category = self.getEntityById(category_id)
                if category is not None and category not in all_categories:
                    all_categories.append(category)

        return all_categories
    
    def getAllAreas(self) -> list[Area]: # * Rumana, Nico fixed this one, working
        all_areas = []

        for categoryQueryHandler in self.categoryQuery:
            areas_df = categoryQueryHandler.getAllAreas()
            if areas_df.empty:
                continue

            for area_id in areas_df["area"]:
                entity = self.getEntityById(area_id)
                area = Area(entity.getIds()[0]) if isinstance(entity, Category) else entity # ?? dealing with the Multidisciplinary case (where the area and category have the same name)
                if area not in all_areas:
                    all_areas.append(area)

        return all_areas
                
    def getCategoriesWithQuartile(self, quartiles: Optional[set[str]]) -> list[Category]: # * Ila, working
        categories_with_quartiles = [] # no need to worry about the no quartiles specified case

        for categoryQueryHandler in self.categoryQuery:
            categories_df = categoryQueryHandler.getCategoriesWithQuartile(quartiles)
            if categories_df.empty:
                continue

            for category_id in categories_df["category"]:
                category = self.getEntityById(category_id)
                if category not in categories_with_quartiles: # keep as a safety measure in the case that a quartile exists in a separate handler
                    categories_with_quartiles.append(category)

        return categories_with_quartiles
        
    def getCategoriesAssignedToAreas(self, areas_ids: set[str]) -> list[Category]: # * Martina, working
        assigned_categories = []

        for categoryQueryHandler in self.categoryQuery: 
            areas_df = categoryQueryHandler.getCategoriesAssignedToAreas(areas_ids)
            
            if areas_df.empty:
                continue

            input_areas = {str(area_id).lower() for area_id in areas_ids}
            match_area = areas_df["area"].astype(str).str.lower().isin(input_areas)
            areas = areas_df[match_area]
            
            for category_id in areas["category"]:
                category = self.getEntityById(category_id)
                if category not in assigned_categories:
                    assigned_categories.append(category)

        return assigned_categories
            
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]: # * Nico, working
        assigned_areas = []

        for categoryQueryHandler in self.categoryQuery:
            categories_df = categoryQueryHandler.getAreasAssignedToCategories(category_ids)
            if categories_df.empty:
                continue
            
            input_categories = {str(category_id).lower() for category_id in category_ids}
            categories_match = categories_df["category"].astype(str).str.lower().isin(input_categories)
            categories = categories_df[categories_match]

            for area_id in categories["area"]:
                entity = self.getEntityById(area_id)
                area = Area(entity.getIds()[0]) if isinstance(entity, Category) else entity # ?? dealing with the Multidisciplinary case (where the area and category have the same name)
                if area not in assigned_areas: # working thanks to the definition of equality in the IdentifiableEntity class
                    assigned_areas.append(area)

        return assigned_areas

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str] = None) -> list[Journal]: # * Nico, works
        target_categories = []
        journals_in_categories = []

        if category_ids: # no duplicates are possible given that category_ids is a set
            for category_id in category_ids: 
                category = self.getEntityById(category_id)
                target_categories.append(category) # only a list with VERY specific categories
        else:
            target_categories = self.getAllCategories() # if unspecified, all categories are assumed
        
        journals = self.getAllJournals()

        for journal in journals:
            journal_categories = journal.getCategories() # this is why we have getter methods!!!
            for journal_category in journal_categories:
                journal_category_quartile = journal_category.getQuartile()
                if journal_category in target_categories and (not quartiles or journal_category_quartile in quartiles): # adding not quartiles in case no quartiles are specified
                    journals_in_categories.append(journal)

        return journals_in_categories
    
    def getJournalsInAreasWithLicense(self, areas_ids: set[str], licenses: set[str]) -> list[Journal]: # Ila 
        result = []
        
        if not areas_ids:
            target_areas = self.getAllAreas()
        else:
            target_areas = [self.getEntityById(area_id) for area_id in areas_ids]
            target_areas = [area for area in target_areas if area is not None]
            target_areas = [Area(entity.getIds()[0]) if isinstance(entity, Category) else entity for entity in target_areas]
            target_areas = [area for area in target_areas if isinstance(area, Area)]

        if not target_areas:
            return []

        jou_with_license = self.getJournalsWithLicense(licenses)

        for jou in jou_with_license:
            if jou is None:
                continue

            journal_areas = jou.getAreas()
            
            for area in journal_areas:
                if area in target_areas:
                    result.append(jou)
                    break
        
        return result
        
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]) -> list[Journal]: # * Ila, Marti & Nico, works
        diamond_journals = []
        all_journals = self.getAllJournals()

        target_areas = self.getAllAreas() if not areas_ids else [self.getEntityById(area) for area in areas_ids] # Ila fixed this part that she wrote wrong
        target_categories = self.getAllCategories() if not category_ids else [self.getEntityById(category) for category in category_ids]
        target_quartiles = None if not quartiles else quartiles

        for journal in filter(lambda journal: not journal.hasAPC(), all_journals):
            if any(area in target_areas for area in journal.getAreas()): # Ila: added any, if one of the areas is True 
                diamond_journals.append(journal)
                continue 

            for category in filter(lambda category: category in target_categories, journal.getCategories()): # Nico – only for the categories that are specified in the input are those that can be checked for quartile eligibility
                if target_quartiles is None or category.getQuartile() in target_quartiles:
                    diamond_journals.append(journal)
                    break  

        return diamond_journals

# ! ––––––––––––––––––––––––––––––––––––––––––––––––––– For testing purposes only ––––––––––––––––––––––––––––––––––––––––––––––––––– !

def getEntitiesFromList(
    entities: list[IdentifiableEntity] | IdentifiableEntity,
    result_type: str,
    associated_journal_objects: bool = False
) -> pd.DataFrame:

    if isinstance(entities, IdentifiableEntity):
        entities = [entities]

    return_result = pd.DataFrame()
    none_results = 0
    additional_objects = []

    for entity in entities:
        if entity is not None:
            if result_type in ("journal", "journals"):
                journal_outputs = [
                    ", ".join(entity.getIds()), 
                    entity.getTitle(), 
                    ", ".join(entity.getLanguages()), 
                    entity.getPublisher(), 
                    entity.hasDOAJSeal(), 
                    entity.getLicense(), 
                    entity.hasAPC()
                ]
                journal_columns = [
                    "journal-ids", 
                    "title", 
                    "languages", 
                    "publisher", 
                    "seal", 
                    "license", 
                    "apc"
                ]
                if return_result.empty:
                    return_result = pd.DataFrame([journal_outputs], columns=journal_columns)
                else:
                    new_result = pd.Series(journal_outputs, index=journal_columns)
                    return_result = pd.concat([return_result, new_result.to_frame().T], ignore_index=True)

                if associated_journal_objects:
                    journal_categories = entity.getCategories()
                    if journal_categories:
                        journal_categories_df = getEntitiesFromList(journal_categories, "category")
                        additional_objects.append(journal_categories_df)

                    journal_areas = entity.getAreas()
                    if journal_areas:
                        journal_areas_df = getEntitiesFromList(journal_areas, "areas")
                        additional_objects.append(journal_areas_df)

            elif result_type in ("category", "categories"):
                category_outputs = [
                    ", ".join(entity.getIds()), 
                    entity.getQuartile()
                ]
                category_columns = ["category", "quartile"]
                if return_result.empty:
                    return_result = pd.DataFrame([category_outputs], columns=category_columns)
                else:
                    new_result = pd.Series(category_outputs, index=category_columns)
                    return_result = pd.concat([return_result, new_result.to_frame().T], ignore_index=True)

            elif result_type in ("area", "areas"):
                area_outputs = [", ".join(entity.getIds())]
                area_columns = ["area"]
                if return_result.empty:
                    return_result = pd.DataFrame([area_outputs], columns=area_columns)
                else:
                    new_result = pd.Series(area_outputs, index=area_columns)
                    return_result = pd.concat([return_result, new_result.to_frame().T], ignore_index=True)
        else:
            none_results += 1

    if none_results:
        print(f"None results: {none_results}")

    if additional_objects:
        for additional_object in additional_objects:
            print(additional_object)

    # ! Ila: makes sure that the result is str, not other types 
    for col in return_result.columns:
        if col not in ("seal", "apc"):
            return_result[col] = return_result[col].astype(str)

    return return_result
