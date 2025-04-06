import rdflib as Graph
from rdflib import RDF, URIRef
import pandas
from pandas import read_csv, Series, DataFrame
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
        if ".csv" in path:
        # graph_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        # jou = JournalUploadHandler() 
        # jou.setDbPathOrUrl(graph_endpoint)
        # jou.pushDataToDb("data/doaj.csv")

            return True 

class JournalUploadHandler(UploadHandler): # handles CSV files
    def __init__(self):
        super.__init__(self)
    
    JournalUploadHandler = URIRef("https://schema.org/Periodical") 
    journal = read_csv("/Users/Martina/Desktop/data_science_project/doaj.csv")
    

    pass

class CategoryUploadHandler(UploadHandler): # handles JSON files
    pass

