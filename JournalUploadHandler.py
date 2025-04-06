from rdflib import Graph
from rdflib import RDF, URIRef, Literal
import pandas
from pandas import read_csv, Series, DataFrame
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

# form pyOptional import Optional 

# class IdentifiableEntity(object):
#     def __init__(self, id:list|str): # one or more strings. Just covering any case 
#         if not isinstance(id, list) or not all(isinstance(i, str) for i in id):
#             raise TypeError(f"Expected a list of str or a str, got {type(id).__name__}")
#         self.id = id 

#     def getIds(self):
#         return list(self.id)

# class Journal(IdentifiableEntity):
#     def __init__(self, id, title: str, languages: str|list, publisher: Optional[str], 
#                  seal: bool, licence: str, apc: bool, hasCategory: Optional[list[Category]], 
#                  hasArea: Optional[list[Area]]):
#         super().__init__(id)

#         if not isinstance(title, str) or not title:
#             raise TypeError(f"Expected a non-empty str, got {type(title).__name__}")
        
#         if not isinstance(languages, list) or not all(isinstance(lang, str) for lang in languages) or not languages:
#             raise TypeError(f"Expected a non-empty str or list, got {type(languages).__name__}")
        
#         if not(isinstance(publisher, str) or isinstance(publisher, None)):
#             raise TypeError(f"Expected a str or a NoneType, got {type(publisher).__name__}")
        
#         if not isinstance(seal, bool):
#             TypeError(f"Expected a boolean, got {type(seal).__name__}")
        
#         if not isinstance(licence, str) or not licence:
#             raise TypeError(f"Expected a non-empty str, got {type(licence).__name__}")
        
#         if not isinstance(apc, bool):
#             raise TypeError(f"Expected a boolean, got {type(apc).__name__}")
        
#         self.title = title
#         self.languages = languages
#         self.publisher = publisher
#         self.seal = seal
#         self.licence = licence
#         self.apc = apc
#         self.hasCategory = hasCategory   # ! List of Category objects, CHECK !
#         self.hasArea =  hasArea # ! List of Area objects, CHECK 

# #      def addCategory(self, category): # ! CHECK if it is necessary!
# #         if not isinstance(category, Category):
# #             raise ValueError("category must be an instance of Category.")
# #         self.categories.append(category)

# #     def addArea(self, area): # same here
# #         if not isinstance(area, Area):
# #             raise ValueError("area must be an instance of Area.")
# #         self.areas.append(area) """

#     def getTitle(self):
#         return self.title

#     def getLanguage(self):
#         return list(self.languages)

#     def getPublisher(self):
#         return self.publisher

#     def hasDOAJSeal(self):
#         return self.seal

#     def getLicence(self):
#         return self.licence

#     def hasAPC(self):
#         return self.apc

#     def getCategories(self):
#         return list(self.hasCategory)

#     def getAreas(self):
#         return list(self.hasArea)


# HANDLERS (note: we can also add other methods, but the contructors do not take any parameter in input)
class Handler(object): 
    pass
    def getDbPathOrUrl(): # str
        pass
    def setDbPathOrUrl(self, pathOrUrl:str):# bool: check if the path is valid
        pass

# MARTINA AND RUMANA: handling CSV in theå UploadHandler
class UploadHandler(Handler):
    def __init__(self):
        pass
    def pushDataToDb(self, path:str): # returns a bool  # if the file is CSV then push data to graph --> if everything is okay returns True
        # if ... this first if checks whether i don't know it's a JSON file, path ends with '.json'
        # elif ... checks if it's a CSV --> then pushing to Graph
        if ".csv" in path:
            jou = JournalUploadHandler() 
            jou.setDbPathOrUrl(graph_endpoint)

            store = SPARQLUpdateStore()

            # The URL of the SPARQL endpoint is the same URL of the Blazegraph
            # instance + '/sparql'
            graph_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

            # It opens the connection with the SPARQL endpoint instance
            store.open((graph_endpoint, graph_endpoint))

            for triple in jou.triples((None, None, None)):  # in this case None means `*` (= anything), 
                store.add(triple)                                # you can also specify conditions if you need (ex. RDF.type)
                
            # Once finished, remember to close the connection
            store.close()
            jou.pushDataToDb("data_science_project/doaj.csv")
            return True 

class JournalUploadHandler(UploadHandler): # handles CSV files
    # according to chatgpt if we don't have to use methods this is not necessary, so check afterwards:
    # def __init__(self):
    #     super.__init__()
    #     self.JournalUploadHandler = URIRef("https://schema.org/Periodical") 
    #     self.journal = read_csv("/Users/Martina/Desktop/data_science_project/doaj.csv")
   
    # Our initial idea of process for creating a graph and associating the class Journal:

        # Journal = URIRef("https://schema.org/Periodical")
        # journal_handler = Journal

        # title = journal_handler.getTitle()
        # title = URIref("https://schema.org/name")

    j_graph = Graph()

    j_path = "/Users/Martina/Desktop/data_science_project/doaj.csv"
    journal = read_csv(j_path, keep_default_na=False)

    journal_df = journal.rename(columns={'Journal title': 'title', 
                                         'Languages in which the journal accepts manuscripts': 'languages', 
                                         'Publisher': 'publisher',
                                         'DOAJ Seal': 'seal',
                                         'Journal license': 'license',
                                         'APC': 'apc'})
    
    journal_df = URIRef("https://schema.org/Periodical") # we still don't know how to reference the entries as a whole as 
                                                         # periodicals and how to reference (if we have to) Journal class
    title = URIRef("https://schema.org/name")
    languages = URIRef ("https://schema.org/Language")
    publisher = URIRef("https://schema.org/publisher")
    # seal = # to be found yet
    license = URIRef("https://schema.org/license")
    # apc = ??
    
    base_URL = "/Users/Martina/Desktop/data_science_project/"
    
    journal_int_id = {}
    for idx, row in journal_df.iterrows():
        loc_id = "journal-" + str(idx)

        subj = URIRef(base_URL + loc_id)

        journal_int_id[row['title']] = subj
        
        j_graph.add((subj, title, Literal(row['title'])))
        j_graph.add((subj, languages, Literal(row['languages'])))
        j_graph.add((subj, publisher, Literal(row['publisher'])))
        j_graph.add((subj, license, Literal(row['license'])))
    
    # print(len(j_graph))

    # maybe we can also realte Journal through RDF owl:equivalentClass???
    # journal = Journal()
    # journal = URIRef("")


class CategoryUploadHandler(UploadHandler): # handles JSON files
    pass


