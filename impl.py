from csv import reader
from pprint import pprint 
from sqlite3 import connect 
from json import load
from pandas import DataFrame, Series, read_csv
import pandas as pd
import re
from rdflib import URIRef, Literal, Graph, RDF
from pyOptional import Optional
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore 

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
        if self.dbPathOrUrl: 
            return True
        else:
            if not pathOrUrl.strip(): 
                return False
            if pathOrUrl.endswith(".db"): # if it is a local path: it is valid
                self.dbPathOrUrl = pathOrUrl
                return True
            elif re.match(r"^https?://[a-zA-Z0-9.-]+(?:\:\d+)?/blazegraph(?:/[\w\-./]*)?/sparql$", pathOrUrl): # if it is a blazegraph url: valid
                self.dbPathOrUrl = pathOrUrl
                return True
            return False # else: not valid

# UPLOAD HANDLER 

class UploadHandler(Handler): # ? da fixare 
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str) -> bool:
        if path.endswith(".json"):
            try:
                cat = CategoryUploadHandler(path)
                cat.setDbPathOrUrl(self.dbPathOrUrl)
                with connect(self.dbPathOrUrl) as conn:
                    cat.categories_df.to_sql(path, conn, if_exists="replace", index=False)
                return True
            except Exception as e:
                print(f"Error uploading data: {e}")
                return False      
        else: 
            try: 
                graph_endpoint = self.pathOrUrl
                jou = JournalUploadHandler() 
                jou.setDbPathOrUrl(graph_endpoint)

                store = SPARQLUpdateStore()
                store.open((graph_endpoint, graph_endpoint))

                for triple in jou.triples((None, None, None)): 
                    store.add(triple) 
                    
                # Once finished, remember to close the connection
                store.close()
                return True 
            except Exception as e:
                print(f"Error uploading data: {e}")
                return False     


class JournalUploadHandler(UploadHandler): # ? check 
    def __init___(self):
        super().__init__() # inherits the "path:str" from the UploadHandler

        # reading the csv with pandas
        journals = read_csv(self.path, 
                  keep_default_na=False,
                  dtype={
                      "Journal Title": "string",
                      "Journal ISSN (print version)": "string",
                      "Languages in which the journal accepts manuscripts": "string",
                      "Publisher": "string",
                      "DOAJ Seal": "string", 
                      "Journal license": "string",
                      "APC": "string"
                  })
        
        # renaming the header
        journals = journals.rename(columns={'Journal title': 'title', 
                                         'Languages in which the journal accepts manuscripts': 'languages', 
                                         'Publisher': 'publisher',
                                         'Journal license': 'license',
                                         'APC': 'apc'})
        
        journal_graph= Graph() # creating the graph that contains everything 
        base_url = "https://comp-data.github.io/res/" # base url
        Journal = URIRef("https://schema.org/ScholarlyArticle")  # URI of the CLASS
        # URIs of the ATTRIBUTES (i.e. headers)
        title = URIRef("https://schema.org/name")
        languages = URIRef ("https://schema.org/Language")
        publisher = URIRef("https://schema.org/publisher")
        doaj_seal= URIRef("https://www.wikidata.org/wiki/Q73548471")
        license = URIRef("https://schema.org/license")
        apc= URIRef("https://www.wikidata.org/wiki/Q15291071")
        
        journals_internal_id= {} # a dictionary with title(name of the journal): baseurl+internal_id(subject)

        for idx, row in journals.iterrows():
            local_id = "journal-"+ str(idx)
            subject= URIRef(base_url + local_id) # ex. "https://comp-data.github.io/res/journal-0"

            journals_internal_id[row["title"]] = subject
        
            journal_graph.add((subject, RDF.type, Journal)) # impostiamo come type il Journal. 1 RDF.type = 1 grafico 
            # per ogni row[""] aggiungi nel grafico l'uri del giornale, l'uri dell'header (titolo, lingua, ecc.) e la str di riferimento
            journal_graph.add((subject, title, Literal(row["title"])))
            journal_graph.add((subject, languages, Literal(row["languages"])))
            journal_graph.add((subject, publisher, Literal(row["publisher"])))
            journal_graph.add((subject, doaj_seal, Literal(row["DOAJ seal"])))
            journal_graph.add((subject, license, Literal(row["license"])))
            journal_graph.add((subject, apc, Literal(row["apc"])))


class CategoryUploadHandler(UploadHandler): # ? check 
    def __init__(self): 
        super().__init__() # inherits the "path:str" from the UploadHandler

        # loading the json file
        with open(self.path, "r", encoding="utf-8") as f:
            json_data= load(f)
            self.json_df = pd.DataFrame(json_data) # casting the json file into a pandas DataFrame

            # creating an internal id for every object in the json file
            df_identifiers= self.json_df[["identifiers"]] 
            internal_ids = []
            for idx, row in df_identifiers.iterrows():
                internal_ids.append("cat-" + str(idx)) # cat-0, cat-1, cat-2, etc.
            

            self.json_df.insert(0, "internal-id", Series(internal_ids, dtype="string")) # inserting into the DF the column "internal-id"
            self.json_df = self.json_df.rename(columns={"identifiers": "journals-ids"}) # renaming the column "identifiers" to "journals-ids", for clarity
            
            # I want to modify the DF self.json_df 'cause the column of the different 'categores' with quartiles is a list of dictionaries, so it is not readable. Let's put them into different rows.
            rows = []
            for _, row in self.json_df.iterrows(): # let's take what we need from the original DF (self.json_df)
                journal_id = row["journals-ids"]  
                internal_id = row["internal-id"] 
                categories = row["categories"]
                areas= row["areas"]

            # appending everything in the list 'rows'
                for cat in categories:
                    for area in areas: 
                        rows.append({
                            "internal-id": internal_id,  
                            "journal-id": journal_id,  
                            "category": cat.get("id"),  # here I changed the 'id' (referred to the category) to 'category', cause it is more understandable
                            "quartile": cat.get("quartile"),
                            "area": area
                        })

            # now the list 'rows' needs to become a DF
            self.categories_df = pd.DataFrame(rows)
            self.categories_df["quartile"] = self.categories_df["quartile"].fillna("N/A") # some categories don't have quartiles. Instead of "None", here we have N/A

# QUERY HANDLER 
class QueryHandler(Handler): 
    super().__init__() 

    def getById(id: str) -> DataFrame: # returns a journal, a caterogy or an area based on the input ID
        pass # Ila

class CategoryQueryHandler(QueryHandler):
    pass
    def getAllCategories() -> DataFrame: # Rumana
        pass
    def getAllAreas() -> DataFrame: # Martina
        pass
    def getCategoriesWithQuartile(quartiles:set[str]) ->DataFrame: # Nico
        pass
    def getCategoriesAssignedToAreas(area_ids: set[str]) -> DataFrame: # Ila
        pass
    def getAreasAssignedToCategories(caterory_ids: set[str]) -> DataFrame: # Rumana
        pass

class JournalQueryHandler(): # all the methods return a DataFrame
    pass
    def getAllJournals(): # Martina
        pass
    def getJournalsWithTitle(self, partialTitle:str): # Nico
        pass
    def getJournalsPublishedBy(self, partialName: str): #Ila
        pass
    def getJournalsWothLicense(self, licenses:set[str]): # Rumana
        pass
    def JournalsWithAPC(): #Martina
        pass
    def JournalsWithDOAJSeal(): # Nico
        pass

#########################

class BasicQueryEngine(object):
    pass
    def cleanJournalHandlers(): # bool 
        pass
    def cleanCategoryHanders(): #bool 
        pass
    # etc. 
    # testing 

# FULL QUERY ENGINE
class FullQueryEngine(BasicQueryEngine): # all the methods return a list of Journal objects
    pass
    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]): 
        pass
    def getJournalsInAreasWithLicense(self, areas_ids:set[str]): 
        pass
    def getDiamondJournalsAreasAmdCAtegoriesWithQuartile(self, areas_ids: set[str], category_ids: set[str], quartiles: set[str]):
        pass

