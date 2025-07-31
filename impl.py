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

class NotImplementedComparisonError(Exception):
    def __init__(self, obj: object, other: object):
        obj_type = type(obj).__name__
        other_type = type(other).__name__
        super().__init__(
            f"Attempted to compare object of type {obj_type!r} and {other_type!r} when this comparison does not exist"
        )

class IdentifiableEntity:
    def __init__(self, id: list[str] | str): 
        if not (isinstance(id, list) and all(isinstance(value, str) for value in id)) and not isinstance(id, str):
            raise TypeMismatchError("a list of strings or a string", id)
        self.id: list[str] | str = id 

    def __eq__(self, other: Self) -> bool: # checking for value equality
        if type(other) is not type(self): 
            raise NotImplementedComparisonError(self, other) 
        return self.id == other.id 
    
    def getIds(self) -> list[str]:
        if isinstance(self.id, str):   
            return [self.id]
        return self.id
    
class Category(IdentifiableEntity):
    def __init__(self, id, quartile: Optional[str]): 
        super().__init__(id) 
        if quartile is not None and not isinstance(quartile, str):
            raise TypeMismatchError("a NoneType or str", quartile)
        self.quartile = quartile 

    def __hash__(self):
        return hash((self.id, self.id))
        
    def getQuartile(self): 
        return self.quartile 

class Area(IdentifiableEntity): 
    def __init__(self, id): 
        super().__init__(id) 

    def __hash__(self):
        return hash((self.id, self.id))

class Journal(IdentifiableEntity):
    def __init__(self, id, title: str, languages: str | list, publisher: Optional[str], 
                 seal: bool, license: str, apc: bool):
        super().__init__(id)
        
        if not isinstance(title, str) and not title:
            raise TypeMismatchError("a non-empty str", title) 
        
        if not isinstance(languages, list) and not all(isinstance(lang, str) for lang in languages) and not isinstance(languages, str) and not languages:
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

    def addArea(self, area): 
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
    
    getLicence = getLicense

    def hasAPC(self):
        return self.apc

    def getCategories(self):
        return self.hasCategory

    def getAreas(self):
        return self.hasArea

class Handler: 
    def __init__(self):
        self.dbPathOrUrl = "" 

    def getDbPathOrUrl(self):  
        return self.dbPathOrUrl 
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:  
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
    def createJournalGraph(self, csv_file: str) -> rdflib.Graph: # Martina & Rumana
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
        apc = rdflib.URIRef("https://schema.org/hasAPC") # invented
            
        
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
        
        
        journals['apc'] = journals['apc'].str.lower().map({'yes': True,'no': False}).fillna(False).astype('bool')
        journals['seal'] = journals['seal'].str.lower().map({'yes': True, 'no': False}).fillna(False).astype('bool')

        base_url = "https://github.com/git-lost-data-science/res/"
            
        journals_int_id = {}
        for idx, row in journals.iterrows():
            loc_id = "journal-" + str(idx)
    
            subj = rdflib.URIRef(base_url + loc_id)
    
            journals_int_id[row["title"]] = subj

            issn = str(row.get("issn", "")).strip() 
            eissn = str(row.get("eissn", "")).strip()

            if issn and eissn:
                combined_ids = f"{issn}, {eissn}"
            elif issn and not eissn:
                combined_ids = str(issn)
            elif eissn and not issn:
                combined_ids = str(eissn)
            else:
                combined_ids = ""
        
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

        absolute_path = os.path.abspath(path) 
        if not os.path.exists(absolute_path):
            return False
        
        try:
            with sqlite3.connect(self.dbPathOrUrl) as con:
                categories_df.to_sql("Category", con, if_exists="replace", index=False)
                con.commit()
            return True
        except sqlite3.Error as e: 
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

    def getById(self, _: str) -> pd.DataFrame: # * Nico
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
    
    def getById(self, id: str) -> pd.DataFrame: # * Nico
        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(, \d{4}-\d{3,4}X?)*$')

        if not journal_id_pattern.match(id): 
            entity_types = ["category", "area", "journal-ids"] 
            for entity_type in entity_types:
                object_df = self.getCategoryObjectsById(id, entity_type)
                if not object_df.empty:
                    return self.createCategoryObject(object_df, entity_type) 
            
        else: # for matching journal ids to their categories and quartiles
            possible_journal_ids = [id] + id.split(", ") # Ila
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

    def getAllCategories(self) -> pd.DataFrame: # * Rumana
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                query = "SELECT DISTINCT category FROM Category;"
                df = pd.read_sql(query, con)
                return df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()
            
    def getAllAreas(self) -> pd.DataFrame: # * Martina
        path = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(path) as con:
                query = "SELECT DISTINCT area FROM Category;" # DISTINCT allows to avoid showing duplicates.
                areas_df = pd.read_sql(query, con)
                return areas_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame() # in order to always return a DataFrame object, even if the queries fails for some reason.   

    def getCategoriesWithQuartile(self, quartiles: Optional[set[str]]) -> pd.DataFrame: # * Nico
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

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame: # * Ila
        # it returns a data frame containing all the categories assigned to particular areas specified as input, with no repetitions. In case the input collection of areas is empty, it is like all areas are actually specified.
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
    
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame: # * Nico
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

    def getById(self, id: str) -> pd.DataFrame: 
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

        SELECT ?id ?title ?publisher ?seal ?license ?apc (GROUP_CONCAT(DISTINCT STR(?language); separator=", ") AS ?languages)
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?id .
            ?s schema:name ?title .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
            ?s schema:inLanguage ?language .

            FILTER CONTAINS(LCASE(STR(?id)), LCASE("{id}"))
        }}
        GROUP BY ?id ?title ?publisher ?seal ?license ?apc
        """
        try:
            titles_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"id": "journal-ids"})
            
            if not titles_df.empty and "languages" in titles_df.columns: # dropping duplicates
                titles_df["languages"] = titles_df["languages"].apply(
                    lambda langs: ", ".join(dict.fromkeys(langs.split(", "))) if isinstance(langs, str) else langs
                )
            
            return titles_df
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getAllJournals(self): # * Martina
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
        
    def getJournalsWithTitle(self, partialTitle: str): # * Nico
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

    def getJournalsPublishedBy(self, partialName: str): # * Ila
        # it returns a data frame containing all the journals that have, as a publisher, any that matches (even partially) with the input string.
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

    def getJournalsWithLicense(self, licenses: set[str]) -> pd.DataFrame: # * Rumana
        endpoint = self.getDbPathOrUrl()
        l_set = {l.strip().lower() for l in licenses}
        filters = []   

        for license_val in l_set:
            license_val_escaped = license_val.replace('"', '\\"')   
            filters.append(f'CONTAINS(LCASE(STR(?license)), "{license_val_escaped}")')  
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
            jou_df = sparql_dataframe.get(endpoint, query, True)
            return jou_df.rename(columns={"id": "journal-ids"})
        except Exception as e:
            self.unexpectedDatabaseError(e)
            return pd.DataFrame()

    def getJournalsWithAPC(self): # * Martina
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
    
    def getJournalsWithDOAJSeal(self): # * Nico
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
    def __init__(self): # Ila 
        self.journalQuery = []
        self.categoryQuery = []

    def cleanJournalHandlers(self) -> bool: # Ila
        self.journalQuery = []
        return True
                 
    def cleanCategoryHandlers(self) -> bool: # Ila
        self.categoryQuery = []
        return True  
         
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool: # * Martina
        try:
            self.journalQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False 
            
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool: # * Nico
        try:
            self.categoryQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False 
        
    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]:  # * Nico
        if not isinstance(id, str): 
            try:
                if not id: 
                    return None
                else:
                    id = str(id)
            except Exception as e:
                print(f"Unexpected error during getEntityById: {e}")
                return None

        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(\s*,\s*\d{4}-\d{3,4}X?)*$')

        if journal_id_pattern.match(id) is not None:  
            journal_found = False

            for journalQueryHandler in self.journalQuery:
                journal_object = journalQueryHandler.getById(id)
                
                if journal_object.empty:
                    
                    journal_object = journalQueryHandler.data[
                        journalQueryHandler.data["journal-ids"].str.contains(id, regex=False, na=False)
                    ]
                
                if not journal_object.empty:
                    journal_found = True
                    journal_object = journal_object.iloc[0]
                    break

            if not journal_found:
                return None
            
            ids_list = [id_.strip() for id_ in journal_object["journal-ids"].split(",")]
            languages_list = [lang.strip() for lang in journal_object["languages"].split(",")]

            journal = Journal(
                ids_list,
                journal_object["title"],
                languages_list,
                journal_object["publisher"],
                bool(journal_object["seal"]),
                journal_object["license"],
                bool(journal_object["apc"])
            )

            matching_category_data_found = False
            journal_category_data = ""

            for categoryQueryHandler in self.categoryQuery:
                journal_category_data = categoryQueryHandler.getById(id)
                if not journal_category_data.empty:
                    matching_category_data_found = True
                    journal_category_data = journal_category_data.iloc[0]
                    break

            if matching_category_data_found:
                categories_with_quartiles = journal_category_data["categories-with-quartiles"]
                area_values = journal_category_data["areas"]

                for category_value, quartile_value in categories_with_quartiles.items():
                    category = Category(category_value, quartile_value)
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

            return None


    def getAllJournals(self) -> list[Journal]: # * Ila
        # it returns a data frame containing all the journals that have, as a publisher, any that matches (even partially) with the input string.
        all_journals = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getAllJournals()
            if journals_df.empty: # it the columns is empty, ignore it, go on 
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal not in all_journals:
                    all_journals.append(journal)
        return all_journals

    def getJournalsWithTitle(self, partialTitle: str) -> list[Journal]: # * Martina
        journals_with_title = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getJournalsWithTitle(partialTitle)
            if journals_df.empty:   
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal is not None and journal not in journals_with_title:
                    journals_with_title.append(journal)

        return journals_with_title

    def getAllJournals(self) -> list[Journal]: # * Ila
        # it returns a data frame containing all the journals that have, as a publisher, any that matches (even partially) with the input string.
        all_journals = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getAllJournals()
            if journals_df.empty: # it the columns is empty, ignore it, go on 
                continue

            for journal_ids in journals_df["journal-ids"]: # the journal-ids is a str for sure
                journal = self.getEntityById(journal_ids)
                if journal not in all_journals:
                    all_journals.append(journal)
        return all_journals
    
        
    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]: # * Nico
        journals_published_by = []

        for journalQueryHandler in self.journalQuery:
            journals_df = journalQueryHandler.getJournalsPublishedBy(partialName)
            if journals_df.empty:   
                continue

            for journal_ids in journals_df["journal-ids"]:
                journal = self.getEntityById(journal_ids)
                if journal is not None and journal not in journals_published_by:
                    journals_published_by.append(journal)

        return journals_published_by

    def getJournalsWithLicense(self, licenses: set[str]) -> list[Journal]: # * Rumana
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
            
    def getJournalsWithAPC(self) -> list[Journal]: # * Ila
        # it returns a list of objects having class Journal containing all the journals in DOAJ that do specify an Article Processing Charge (APC).
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
            
    def getJournalsWithDOAJSeal(self) -> list[Journal]: # * Martina
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

    def getAllCategories(self) -> list[Category]: # * Nico
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
    
    def getAllAreas(self) -> list[Area]: # * Rumana
        all_areas = []

        for categoryQueryHandler in self.categoryQuery:
            areas_df = categoryQueryHandler.getAllAreas()
            if areas_df.empty:
                continue

            for area_id in areas_df["area"]:
                entity = self.getEntityById(area_id)
                area = Area(entity.getIds()[0]) if isinstance(entity, Category) else entity # dealing with cases where the area and category have the same name
                if area is not None and area not in all_areas:
                    all_areas.append(area)

        return all_areas
                
    def getCategoriesWithQuartile(self, quartiles: set[str] = None) -> list[Category]: # * Ila
        #  it returns a list of objects having class Category containing all the categories in Scimago Journal Rank having specified, as input, particular quartiles, with no repetitions. In case the input collection of quartiles is empty, it is like all quartiles are actually specified.
        categories_with_quartiles = [] 

        for categoryQueryHandler in self.categoryQuery:
            categories_df = categoryQueryHandler.getCategoriesWithQuartile(quartiles)
            if categories_df.empty:
                continue

            for category_id in categories_df["category"]:
                category = self.getEntityById(category_id)
                if category is not None and category not in categories_with_quartiles: # in the case that a quartile exists in a separate handler
                    categories_with_quartiles.append(category)

        return categories_with_quartiles
        
    def getCategoriesAssignedToAreas(self, areas_ids: set[str]) -> list[Category]: # * Martina
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
                if category is not None and category not in assigned_categories:
                    assigned_categories.append(category)

        return assigned_categories
            
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]: # * Nico
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
                area = Area(entity.getIds()[0]) if isinstance(entity, Category) else entity 
                if area is not None and area not in assigned_areas: 
                    assigned_areas.append(area)

        return assigned_areas

class FullQueryEngine(BasicQueryEngine): 
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]) -> list[Journal]: # * Nico
        # ! The overall amount of journals returned is less (of a few units) than the one expected.
        journals_in_categories = []

        target_categories = self.getAllCategories() if not category_ids else [self.getEntityById(category) for category in category_ids]
        target_categories = list(filter(None, target_categories)) 

        if not quartiles or quartiles == {"Q1", "Q2", "Q3", "Q4"}:
            target_quartiles = None
        elif not quartiles.issubset({"Q1", "Q2", "Q3", "Q4"}): 
            target_quartiles = None
        else:
            target_quartiles = quartiles

        for journal in self.getAllJournals():
            journal_categories = journal.getCategories()
            if journal_categories is None:
                continue

            for journal_category in journal_categories:
                journal_category_quartile = journal_category.getQuartile()
                category_is_match = journal_category in target_categories
                quartiles_match = (
                    target_quartiles is None 
                    or journal_category_quartile is None 
                    or journal_category_quartile in target_quartiles
                )
                
                if category_is_match and quartiles_match and journal not in journals_in_categories:
                    journals_in_categories.append(journal)
                    break

        return journals_in_categories
    
    def getJournalsInAreasWithLicense(self, areas_ids: set[str], licenses: set[str]) -> list[Journal]: # * Ila
        # it returns a list of objects having class Journal containing all the journals in DOAJ with at least one of the licenses specific as input, and that have at least one of the input areas specified in Scimago Journal Rank, with no repetitions. In case the input collection of areas/licenses are empty, it is like all areas/licenses are actually specified.
        journals_with_licenses = []
        
        target_areas = self.getAllAreas() if not areas_ids else [self.getEntityById(area) for area in areas_ids]
        target_areas = list(filter(None, target_areas))

        for journal in self.getJournalsWithLicense(licenses):
            journal_areas = journal.getAreas() 
            if journal_areas is None:
                continue

            for journal_area in journal_areas:
                if journal not in journals_with_licenses and journal_area in target_areas: 
                    journals_with_licenses.append(journal)
                    break

        return journals_with_licenses
        
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]) -> list[Journal]:
        diamond_journals = []

        target_areas = self.getAllAreas() if not areas_ids else [self.getEntityById(area) for area in areas_ids] 
        target_categories = self.getAllCategories() if not category_ids else [self.getEntityById(category) for category in category_ids]
        
        if not quartiles: 
            target_quartiles = None
        elif not quartiles.issubset({"Q1", "Q2", "Q3", "Q4"}): 
            target_quartiles = None
        else:
            target_quartiles = quartiles

        target_areas = list(filter(None, target_areas)) 
        target_categories = list(filter(None, target_categories))

        for journal in filter(lambda j: not j.hasAPC(), self.getAllJournals()): 
            valid_categories_and_quartiles = False
            valid_areas = False

            if target_areas is not None:
                for journal_area in journal.getAreas():
                    if journal_area in target_areas:
                        valid_areas = True
                        break

            for journal_category in journal.getCategories():
                journal_category_quartile = journal_category.getQuartile()
                category_is_match = journal_category in target_categories
                quartiles_match = (
                    target_quartiles is None 
                    or journal_category_quartile is None 
                    or journal_category_quartile in target_quartiles
                )
                
                if category_is_match and quartiles_match:
                    valid_categories_and_quartiles = True
                    break
            
            if valid_categories_and_quartiles and valid_areas and journal not in diamond_journals: 
                diamond_journals.append(journal)

        return diamond_journals # per sicurezza
        

# ! for testing purposes 
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
        if col not in ("journal-ids", "seal", "apc"):
            return_result[col] = return_result[col].astype(str)

    return return_result
