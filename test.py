# import impl
from pprint import pprint

from impl import CategoryUploadHandler, JournalUploadHandler, TypeMismatchError, CategoryQueryHandler

# import os
# print("Current working directory:", os.getcwd())  # Check where the script runs
# print("File exists:", os.path.exists("/Users/Martina/Desktop/data_science_project/scimago.json"))  # Check file visibility

rel_path = "relational.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)
cat.pushDataToDb("/Users/Martina/Desktop/datasci-project/scimago.json") 

graph_endp = "http://localhost:9999/blazegraph/sparql"
jou = JournalUploadHandler()
jou.setDbPathOrUrl(graph_endp)
jou.pushDataToDb("/Users/Martina/Desktop/datasci-project/doaj.csv")

# Testing a query:

# 1) Creating the connection to the QueryHandler:
cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

