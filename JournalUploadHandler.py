import pandas
from pandas import read_csv, Series, DataFrame, read_json
from rdflib import RDF, URIRef, Literal, Graph
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

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
        if path.endswith(".csv"):
            journal = JournalUploadHandler() 
            journal.setDbPathOrUrl(graph_endpoint)

            store = SPARQLUpdateStore()

            graph_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

            store.open((graph_endpoint, graph_endpoint))

            for triple in journal.triples((None, None, None)):  
                store.add(triple)                           
           
            store.close()
            # journal.pushDataToDb("data_science_project/doaj.csv") # ? What is this?
            return True 

class JournalUploadHandler(UploadHandler): # handles CSV files 
    # initiating an empty graph:
    j_graph = Graph()
        
    # referencing all the classes:    
    Journal = URIRef("https://schema.org/Periodical")   # I think I'm referencing in this way directly to the class Journal 
                                                        # if I'm not wrong
    Category = URIRef("https://schema.org/category")    # NOT SURE                   
    Area = URIRef("https://schema.org/subjectOf")   # NOT SURE

    # referencing the attributes:
    title = URIRef("https://schema.org/name")
    languages = URIRef ("https://schema.org/inLanguage") # (superseded /Language)
    publisher = URIRef("https://schema.org/publisher")
    seal = URIRef("http://doaj.org/static/doaj/doajArticles.xsd")    
    license = URIRef("https://schema.org/license")
    apc = URIRef("https://shcema.org/isAccessibleForFree") # this is a boolean value in theory so it should work.
    quartile = URIRef()  # NOT SURE -- this is actually a pain in the ASS to reference

    # referencing the relations:  NOT SURE -- or maybe this is OK but I don't need the classes and the attributes (of Category and Area)?
    hasCategory = URIRef("http://purl.org/dc/terms/isPartOf")
    hasArea = URIRef("http://purl.org/dc/terms/subject") # the hasArea better fits the idea of 'subject' rather than hasCategory
        
    j_path = "/Users/Martina/Desktop/data_science_project/doaj.csv"
    journals = read_csv(j_path, 
                       keep_default_na=False, 
                       dtype={
                              'Journal title': 'string', 
                              'Languages in which the journal accepts manuscripts': 'string', 
                              'Publisher': 'string',
                              'DOAJ Seal': 'bool',
                              'Journal license': 'string',
                              'APC': 'bool' 
                       })
    journals = journals.rename(columns={'Journal title': 'title', 
                                         'Languages in which the journal accepts manuscripts': 'languages', 
                                         'Publisher': 'publisher',
                                         'DOAJ Seal': 'seal',
                                         'Journal license': 'license',
                                         'APC': 'apc'})

    base_url = "/Users/Martina/Desktop/data_science_project/res"  # This probably needs to be fixed in order to be more generally accessible

    # THIS PART IS ADDED AFTERWARDS --
    json = read_json("/data_science_project/scimago.json", lines=True)
        
    category_int_id = {}
    for idx, row in json.items():
        loc_id = "category-" + str(idx)

        subj = URIRef(base_url + loc_id)

        category_int_id[row['categories']] = subj

        j_graph.add((subj, RDF.type, hasCategory))
        # j_graph.add((subj, quartile, Literal[row['quartile']]))  --  This should be added if ONLY I had a damn URIRef #

     area_int_id = {}
     for idx, row in json.items():
        loc_id = "area-" + str(idx)

        subj = URIRef(base_url + loc_id)
        
        area_int_id[row['area']] = subj

        j_graph.add((subj, RDF.type, hasArea))
     
    # -- THIS PART IS ADDED AFTERWARDS.
        
    journals_int_id = {}
    for idx, row in journals.iterrows():
        loc_id = "journal-" + str(idx)

        subj = URIRef(base_URL + loc_id)

        journals_int_id[row['title']] = subj

        j_grap.add((subj, RDF.type, Journal))
        j_graph.add((subj, title, Literal(row['title'])))
        j_graph.add((subj, languages, Literal(row['languages'].split(','))))
        j_graph.add((subj, publisher, Literal(row['publisher'])))
        j_graph.add((subj, seal, Literal(row['seal'])))    
        j_graph.add((subj, license, Literal(row['license'])))
        j_graph.add((subj, apc, Literal(row['apc']))) 
        # THIS PART IS ADDED AFTERWARDS --    
        j_graph.add((subj, hasCategory, category_int_id[row['categories']]))  # HERE I'm technically missing just the @quartile
        j_graph.add((subj, hasArea, area_int_id[row['area']]))
        # -- THIS PART IS ADDED AFTWERWARDS. 
    # print(len(j_graph))
    # technically with the idea we had before, this ^ should be all, but maybe not... 
    # [I'm adding the processing for also the '# NOT SURE' categories -- BEFORE the journal impl]

class CategoryUploadHandler(UploadHandler): # handles JSON files
    pass


