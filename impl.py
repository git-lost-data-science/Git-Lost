import json
import re
from typing import Optional, Self
import os

import rdflib
import pandas as pd
import sqlite3 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import sparql_dataframe 

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
        # Nico: I do wonder if it would be more correct

    def __eq__(self, other: Self) -> bool: # checking for value equality
        return self.id == other.id 
    
    def getIds(self) -> list[str]:
        if isinstance(self.id, str):
            return [self.id] # a one element list (containing a string)
        return list(self.id) # returning the list as per usual
    
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

        if not isinstance(title, str) and not title:
            raise TypeMismatchError("a non-empty str", title) # ? the same as raise TypeError(f"Expected a non-empty str, got {type(title).__name__}")
        
        if not isinstance(languages, list) and not all(isinstance(lang, str) for lang in languages) and not isinstance(languages, str) and not languages:
            raise TypeMismatchError("a non-empty str or list", languages)
        
        if not isinstance(publisher, str) and not publisher:
            raise TypeMismatchError("a str or a NoneType", publisher)
        
        if not isinstance(seal, bool) and not isinstance(seal, numpy.bool):
            raise TypeMismatchError("a boolean", seal)
        
        if not isinstance(license, str) and not license:
            raise TypeMismatchError("a non-empty str", license)
        
        if not isinstance(apc, bool):
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
        return [self.languages]

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
        self.dbPathOrUrl = pathOrUrl # should reset the path every time unconditionally
            
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

    def pushDataToDb(self, path: str) -> bool: # Nico & Martina
        absolute_path = os.path.abspath(path) # Ila: os uses always an absolute path
        if not os.path.exists(absolute_path):
            return False

        if path.endswith(".csv") and isinstance(self, JournalUploadHandler): # as per Gemini's suggestion, each upload handler can now handle files independently of one another
            return self.pushDataToDb(path)
        elif path.endswith(".json") and isinstance(self, CategoryUploadHandler):
            return self.pushDataToDb(path)
        else:
            return False

class JournalUploadHandler(UploadHandler):
    def _csv_file_to_graph(self, csv_file: str) -> rdflib.Graph: # Martina
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
            # language = str(row.get('languages')).split(', ') # ! Please change this, it is not working -- Will it work now? We hope
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

            current_languages_str = str(row.get("languages", "")).strip() 
            if current_languages_str: 
                list_of_languages = [lang.strip() for lang in current_languages_str.split(",")]
                for lang_item in list_of_languages:
                    if lang_item: 
                        j_graph.add((subj, languages, rdflib.Literal(lang_item)))
        
            j_graph.add((subj, rdflib.RDF.type, Journal))
            j_graph.add((subj, id, rdflib.Literal(combined_ids)))
            j_graph.add((subj, title, rdflib.Literal(row["title"])))
            # j_graph.add((subj, languages, rdflib.Literal(row["languages"])))
            j_graph.add((subj, publisher, rdflib.Literal(row["publisher"])))
            j_graph.add((subj, seal, rdflib.Literal(row["seal"])))    
            j_graph.add((subj, license, rdflib.Literal(row["license"])))
            j_graph.add((subj, apc, rdflib.Literal(row["apc"]))) 
        return j_graph
    
    def pushDataToDb(self, path: str) -> bool: 
        jou_graph = self._csv_file_to_graph(path)
        try:
            store = SPARQLUpdateStore()
            store.open((self.dbPathOrUrl, self.dbPathOrUrl))
            for triple in jou_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            return True
        except Exception as e:
            print(f"Error during pushDataToDb (CSV to Blazegraph): {e}")
            return False

class CategoryUploadHandler(UploadHandler): 
    def _json_file_to_df(self, json_file: str) -> pd.DataFrame: # Ila
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
    
    def pushDataToDb(self, path: str) -> bool: # Nico & Martina
        categories_df = self._json_file_to_df(path)
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

# ! NOTE: TABLE NAME IS 'Category' NOT 'Categories'
# QUERY HANDLER 
class QueryHandler(Handler): 
    def __init__(self):
        super().__init__()

    def getById(self, id: str) -> pd.DataFrame: # ?? Nico
        if isinstance(self, CategoryQueryHandler): # perfect compatibility – there will no longer be issues
            return self.getById(id)
        elif isinstance(self, JournalQueryHandler):
            return self.getById(id)
        else: # effectively addresses Peroni's "just_a_test" input
            return pd.DataFrame()

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, id: str) -> pd.DataFrame: 
        journal_id_pattern = re.compile(r'^\d{4}-\d{3,4}X?(, \d{4}-\d{3,4}X?)*$')

        if not bool(journal_id_pattern.match(id)): 
            categories_df = self.getCategoryObjectsById(id, "category")
            areas_df = self.getCategoryObjectsById(id, "area")
            if not categories_df.empty:
                categories_df = self.createCategoryObject(categories_df, "category")
                return categories_df
            elif not areas_df.empty:
                areas_df = self.createCategoryObject(areas_df, "area")
                return areas_df
        else: # for matching journal ids to their categories and quartiles
            possible_journal_ids = id.split(", ")
            possible_journal_ids.insert(0, id) # inserting the id at the start so it is tested first
            for journal_id in possible_journal_ids:
                journal_ids_df = self.getCategoryObjectsById(journal_id, "journal-ids")
                if not journal_ids_df.empty:
                    journal_ids_df = self.createCategoryObject(journal_ids_df, "journal-ids")
                    return journal_ids_df
            return pd.DataFrame()
    
    def createCategoryObject(self, target_df: pd.DataFrame, entity_type: str) -> pd.Series: # ^ N method to avoid redundant repetition of code, homogenises data
        if entity_type == 'journal-ids':
            categories_with_quartiles = {}
            areas = set()
            for _, row in target_df.iterrows():
                categories_with_quartiles[row["category"]] = row.get("quartile")
                areas.add(row.get("area"))

            category_object_values = [target_df.iloc[0]["journal-ids"], categories_with_quartiles, areas]
            category_object = pd.DataFrame([category_object_values], columns=["journal-ids", "categories-with-quartiles", "areas"])
            return category_object

        else: 
            categories = list(set(row.get("category") for _, row in target_df.iterrows())) # sets to prevent duplicates
            areas = list(set(row.get("area") for _, row in target_df.iterrows()))
            unique_quartiles = list(set(row.get("quartile") for _, row in target_df.iterrows() if row.get("quartile") is not None))
            journal_ids = list(set(row.get("journal-ids") for _, row in target_df.iterrows())) 

            if not unique_quartiles:
                quartiles = None
            elif len(unique_quartiles) == 1:
                quartiles = unique_quartiles[0]
            elif len(unique_quartiles) < 4: 
                quartiles = ", ".join(sorted(unique_quartiles)) 
            else: 
                quartiles = None

            # constructing a new dataframe
            category_object_values = [entity_type, journal_ids, ", ".join(categories), quartiles, ", ".join(areas)]
            category_object = pd.DataFrame([category_object_values], columns=["entity-type", "journal-ids", "category", "quartile", "area"])
            return category_object

    def getCategoryObjectsById(self, id: str, entity_type: str) -> pd.DataFrame: # ^ N secondary function
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
            print(f"Error in the query: {e}")
            return pd.DataFrame()


    def getAllCategories(self) -> pd.DataFrame: # Rumana
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                query = "SELECT DISTINCT * FROM Category;"
                df = pd.read_sql(query, con)
                return df
        except Exception as e:
            print(f"Connection to SQL database failed due to error: {e}")
            return pd.DataFrame()
            
    def getAllAreas(self) -> pd.DataFrame: # Martina
        try:
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                q2 = "SELECT DISTINCT area FROM Category;" # DISTINCT allows to avoid showing duplicates.
                q2_df = pd.read_sql(q2, con)
                return q2_df
        except Exception as e:
                print(f"Connection to SQL database failed due to error: {e}") 
                return pd.DataFrame() # in order to always return a DataFrame object, even if the queries fails for some reason.   

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame: # ! Nico is finished this method, requires testing
        path = self.getDbPathOrUrl() # a safer way to access the path than directly accessing the variable
        categories = [] 

        try:
            with sqlite3.connect(path) as con:
                if not quartiles:
                    query = """
                    SELECT DISTINCT category, quartile 
                    FROM Category;
                    """
                    category_df = pd.read_sql(query, con)
                    categories.append(category_df)
                else:
                    query_template = """
                    SELECT DISTINCT category, quartile 
                    FROM Category 
                    WHERE quartile = ?;
                    """
                    for quartile_value in quartiles: # ? Testing one quartile at a time, addresses the blank case
                        category_df = pd.read_sql(query_template, con, params=(quartile_value,))
                        categories.append(category_df)
            
            if not categories:
                return pd.DataFrame()

            all_categories_df = pd.concat(categories, ignore_index=True).drop_duplicates()
            return all_categories_df
        except Exception as e:
            print(f"Error in the query: {e}") 
            return pd.DataFrame()  

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame: # ? Ila, it works T.T
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
                    df = pd.read_sql(query, con, params=[f"%{a}%" for a in area_ids_lower])
                else:
                    query = """
                        SELECT DISTINCT area, category
                        FROM Category
                    """
                    df = pd.read_sql(query, con)
                return df
        except Exception as e:
            print(f"Error in the query: {e}")
            return pd.DataFrame()
    
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame: # ?? Nico, requires testing
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
            return pd.DataFrame()

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

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

    def getJournalById(self, id: str) -> pd.DataFrame: # ^ N supplementary method
        endpoint = self.getDbPathOrUrl()
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?o . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .
        
            FILTER CONTAINS(LCASE(STR(?o)), LCASE("{id}"))
        }} 
        """

        try:
            titles_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"o": "journal-ids"})
            return titles_df
        except Exception as e:
            print(f"Error in the SPARQL query: {e}")
            return pd.DataFrame()

    def getAllJournals(self): # Martina
        try:
            endpoint = self.getDbPathOrUrl()
            journal_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
            WHERE {{ 
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?o . 
                ?s schema:name ?title . 
                ?s schema:inLanguage ?languages .
                ?s schema:publisher ?publisher .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
            }} 
            """
            
            journal_df = sparql_dataframe.get(endpoint, journal_query, True).rename(columns={"o": "journal-ids"})
            return journal_df
        
        except Exception as e:
            print(f"Error in SPARQL query due to: {e}") 
            return pd.DataFrame()
        
    def getJournalsWithTitle(self, partialTitle: str): # ?? Nico, requires testing
        endpoint = self.getDbPathOrUrl()
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?o . 
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
            titles_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"o": "journal-ids"})
            return titles_df
        except Exception as e:
            print(f"Error in the SPARQL query: {e}")
            return pd.DataFrame()

    def getJournalsPublishedBy(self, partialName: str): # ? Ila : it works
        endpoint = self.getDbPathOrUrl()        
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
        WHERE {{ 
            ?s rdf:type schema:Periodical .
            ?s schema:identifier ?o . 
            ?s schema:name ?title . 
            ?s schema:inLanguage ?languages .
            ?s schema:publisher ?publisher .
            ?s schema:hasDOAJSeal ?seal .
            ?s schema:license ?license .
            ?s schema:hasAPC ?apc .

            FILTER CONTAINS(LCASE(STR(?title)), LCASE("{partialName}"))
        }} 
        }}
        """
        try:
            df = sparql_dataframe.get(endpoint, query, True).rename(columns={"o": "journal-ids"})
            return df
        except Exception as e:
            print(f"Error in SPARQL query: {e}")
            return pd.DataFrame()
            
    def getJournalsWithLicense(self, licenses: set[str]): # Rumana
        endpoint = self.getDbPathOrUrl()
        try:
            license_list = '", "'.join(licenses)
            query = f"""
            PREFIX res:    <https://github.com/git-lost-data-science/res/>
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
            WHERE {{ 
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?o . 
                ?s schema:name ?title . 
                ?s schema:inLanguage ?languages .
                ?s schema:publisher ?publisher .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .

                FILTER (?license IN ("{license_list}"))
            }}
            """
            jou_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"o": "journal-ids"})
            return jou_df.fillna('')  

        except Exception as e:      
            print(f"Connection to SPARQL endpoint failed due to error: {e}") 
            return pd.DataFrame()

            
    def getJournalsWithAPC(self): # Martina
        try:
            endpoint = self.getDbPathOrUrl()
            jouAPC_query = """
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
            WHERE {{
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?o . 
                ?s schema:name ?title . 
                ?s schema:inLanguage ?languages .
                ?s schema:publisher ?publisher .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
                
                FILTER (?apc = true)
            }}
            """
            # After a million times of loading the data the query works correctly filtering ONLY the Journals with APC (apc = true)
            # if this was actually what was required...
            
            jouAPC_df = sparql_dataframe.get(endpoint, jouAPC_query, True).rename(columns={"o": "journal-ids"})
            return jouAPC_df
        
        except Exception as e:
            print(f"Error in SPARQL query due to: {e}") 
            return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self): # ?? Nico, requires testing
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?o ?title ?languages ?publisher ?seal ?license ?apc
            WHERE {
                ?s rdf:type schema:Periodical .
                ?s schema:identifier ?o . 
                ?s schema:name ?title . 
                ?s schema:inLanguage ?languages .
                ?s schema:publisher ?publisher .
                ?s schema:hasDOAJSeal ?seal .
                ?s schema:license ?license .
                ?s schema:hasAPC ?apc .
                
                FILTER (?seal = true)
            }
            """
            
            journal_DOAJ_df = sparql_dataframe.get(endpoint, query, True).rename(columns={"o": "journal-ids"})
            return journal_DOAJ_df
        
        except Exception as e:
            print(f"The query was unsuccessful due to the following error: {e}") 
            return pd.DataFrame()

class BasicQueryEngine:
    def __init__(self): 
        self.journalQuery = []
        self.categoryQuery = []

    def cleanJournalHandlers(self) -> bool: # ? Ila, done
        self.journalQuery = []
        return True
                 
    def cleanCategoryHandlers(self) -> bool: #  ? Ila, done
        self.categoryQuery = []
        return True  
         
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool: # ? Martina, done
        try:
            self.journalQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the journal handler to the journal handlers
            
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool: # * Nico, done
        try:
            self.categoryQuery.append(handler)
            return True
        except Exception as e:
            print(f"Error loading methods due to the following: {e}")
            return False # appends the category handlers to the categoryQuery

    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]: # ?? Nico
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
        
            matching_category_data_found = False

            for categoryQueryHandler in self.categoryQuery:
                journal_category_data = categoryQueryHandler.getById(id)
                if not journal_category_data.empty:
                    matching_category_data_found = True
                    journal_category_data = journal_category_data.iloc[0]
                    break
            
            if not matching_category_data_found:
                return None
            
            categories_with_quartiles = journal_category_data["categories-with-quartiles"]
            area_values = journal_category_data["areas"]

            journal = Journal( 
                journal_object["journal-ids"], 
                journal_object["title"], 
                journal_object["languages"], 
                journal_object["publisher"], 
                bool(journal_object["seal"]), # stored as a numpy bool, needs to be converted
                journal_object["license"], 
                bool(journal_object["apc"])
            )

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
                if not category_object.empty:
                    if category_object["entity-type"] == "category":
                        category_object = category_object.iloc[0]
                        return Category(category_object["category"], category_object["quartile"])
                    elif category_object["entity-type"] == "area":
                        category_object = category_object.iloc[0]
                        return Area(category_object["area"])
                else:
                    return None


    def getAllJournals(self) -> list[Journal]: # ! Ila, requires fixing
        all_data = []

        for journal in self.journalQuery:  # it looks at all the queries in the list journalQuery 
            df = journal.getAllJournals()  # calls the previous method on it
            all_data.append(df) # appends the result of that method in a list

        if all_data:
            db = pd.concat(all_data, ignore_index=True).drop_duplicates().fillna("")

            result = []
            for _, row in db.iterrows():
                journal_id = row.get("id")  
                journal = self.getEntityById(journal_id)
                if journal:
                    result.append(journal)

            return result
        else:
            return []

    def getJournalsWithTitle(self, partialTitle:str) ->list[Journal]: # ! Martina, requires fixing
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
        
    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]: # ?? Nico, requires testing
        published_journals = []

        for journalQueryHandler in self.journalQuery:
            journal_df = journalQueryHandler.getJournalsPublishedBy(partialName)
            if not journal_df.empty:
                for value in journal_df["journal-ids"]:
                    journal = self.getEntityById(value)
                    published_journals.append(journal)

        return published_journals

    def getJournalsWithLicense(self, licenses:set[str]) -> list[Journal]: # ! Rumana, requires fixing
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
            db['license'] = db['license'].str.strip()
            db = db[db['license'].isin(licenses)]
    
            result = []
            for _, row in db.iterrows():
                journal_id = row.get('id')
                journal = self.getEntityById(journal_id)
                if journal:
                    result.append(journal)
    
            return result
    
        except Exception as e:
            print(f"Error while getting journals by license: {e}")
            return []
            
    def getJournalsWithAPC(self) -> list[Journal]: # Ila
        all_data = []
        result = []

        for journal in self.journalQuery:  # it looks at all the queries in the list journalQuery
                df = journal.getJournalsWithAPC()  # calls the previous method on it
                all_data.append(df) # appends the result of that method in a list

        if all_data:
            all_data = pd.concat(all_data, ignore_index=True).drop_duplicates().fillna("")

            for _, row in all_data.iterrows():
                journal_id = row.get("id")  
                journal = self.getEntityById(journal_id)
                if journal:
                    result.append(journal)

            return result
        else:
            return []
            
    def getJournalsWithDOAJSeal(self) -> list[Journal]: # ! Martina, requires fixing
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

    def getAllCategories(self) -> list[Category]: # * Nico, fully working! Copy this structure in your own models
        all_categories = []

        for categoryQueryHandler in self.categoryQuery:
            category_df = categoryQueryHandler.getAllCategories()
            if not category_df.empty:
                for value in category_df["category"]:
                    category = self.getEntityById(value)
                    all_categories.append(category)

        return all_categories
    
    def getAllAreas(self) -> list[Area]: # ! Rumana, requires fixing 
        area_dfs = [category.getAllAreas() for category in self.categoryQuery if isinstance(category, CategoryQueryHandler)]
        all_areas = []
        if area_dfs:
            db = pd.concat(area_dfs, ignore_index=True).drop_duplicates().fillna('')
            for _, row in db.iterrows():
                area_id = row.get('area')
                area = self.getEntityById(area_id)
                if area:
                    all_areas.append(area)
        return all_areas
                
    def getCategoriesWithQuartile(self, quartiles:set[str]) -> list[Category]: # ! Ila, requires fixing
        all_data = []
        for category in self.categoryQuery:  # it looks at all the queries in the list journalQuery
                df = category.getCategoriesWithQuartile()  # calls the previous method on it
                all_data.append(df) # appends the result of that method in a list
        
        if all_data:
            db = pd.concat(all_data, ignore_index=True).drop_duplicates().fillna("")

            result = []
            for _, row in db.iterrows():
                category_id = row.get("category")  
                category = self.getEntityById(category_id)
                if category.quartile in quartiles:
                    result.append(category)
            return result
        else:
            return []
        
    def getCategoriesAssignedToAreas(self, areas_ids: set[str]) -> list[Category]: # ! Martina, requires fixing
        cat_areas = []
        assigned_cat = []

        for categoryQueryHandler in self.categoryQuery: 
            category_df = categoryQueryHandler.getCategoriesAssignedToAreas(areas_ids)
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
            
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]: # * Nico, done and working 
        assigned_areas = []

        for categoryQueryHandler in self.categoryQuery:
            category_df = categoryQueryHandler.getAreasAssignedToCategories()
            if not category_df.empty:
                category_match = {str(category_id).lower() for category_id in category_ids}
                categories_match = category_df["category"].astype(str).str.lower().isin(category_match)
                categories_df = categories_df[categories_match]

                for area_name in category_df["area"]:
                    area = self.getEntityById(area_name)
                    if area not in assigned_areas: # working thanks to the definition of equality in the IdentifiableEntity class
                        assigned_areas.append(area)

        return assigned_areas

class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]=None) -> list[Journal]: # ?? Nico, requires testing once getEntityById works
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

    def getJournalsInAreasWithLicense(self, areas_ids:set[str], licenses: set[str]) -> list[Journal]: # Ila 
        if not areas_ids and not licenses: # if there are no areas nor licenses specified, then all the journal objects are returned 
            return self.getAllJournals()
        
        if not areas_ids: # if there are no areas specified, then all the journal with the specified licenses are returned
            return self.getJournalsWithLicense(licenses)
        
        journals_with_license = self.getJournalsWithLicense(licenses) # else, we need to return all the Journal objects that have a license and from those journals, take all the journals in the specified area
        result = []

        for journal in journals_with_license:
            journal_areas = journal.getAreas() # getting each area
            for area in journal_areas:
                if area in areas_ids:
                    result.append(journal)
        return result
    
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]) -> list[Journal]: # Martina
        diamond_journals = []

        all_journals = self.getAllJournals()
        target_areas = [self.getAllAreas() if areas_ids else self.getEntityById(area) for area in areas_ids]
        target_categories = [self.getAllCategories() if category_ids else self.getEntityById(category) for category in category_ids]
        target_quartiles = None if not quartiles else quartiles 

        for journal in filter(lambda journal: not journal.hasAPC(), all_journals): # only journals without an APC
            journal_areas = journal.getAreas() # list of all the areas of the Journal
            for area in journal_areas:
                if area in target_areas and journal not in diamond_journals:
                    diamond_journals.append(journal)
                    break
            journal_categories = journal.getCategories() # list of all the Categories of the journal
            for category in journal_categories:
                if category in target_categories and journal not in diamond_journals:
                    diamond_journals.append(journal)
                    break
                journal_quartiles = category.getQuartile()
                for quartile in journal_quartiles:
                    if quartile in target_quartiles and journal not in diamond_journals: 
                        diamond_journals.append(journal)
                        break
        return diamond_journals
