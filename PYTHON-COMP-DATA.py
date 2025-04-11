# COMP-DATA 

# CSV #  each row represents a (subject) entity and each cell defines the (object) value associated to that entity via the predicate defined by the column label, if specified. 

# opening a CSV file
from csv import reader

with open("01-publications.csv", "r", encoding="utf-8") as f: # la funzione open in realtà prende anche più argomenti
publications = reader(f)
next(publications)  # it skip the first row of the CSV table, se vuoi evitare gli headers e andare direttamente ai dati della tabella

# curiosità::: all file objects one wants to create in Python to process files stored in the file system must be also closed once all the operations on that file 
# are concluded. The keyword with used with the function open allows one to handle the opening and closing of the file object automatically. 

from pprint import pprint
for row in publications:
    print(row)

# NB!!!! once you have iterated it once, the CSV reader is C O N S U M E D  and does not allow you to iterate over the same object twice. 
# Come fare ad iterare le rows più di una volta? Trasformi il cvs file in una lista python! Così puoi fare tutte le operazioni che fanno le liste
# puoi accedere alle liste con list[n], puoi .append eccetera
# il file csv è una lista di liste (le row, orizzontali )

with open("01-publications.csv", "r", encoding="utf-8") as f:
    publications = reader(f)
    publications_list = list(publications) # L I S T A < 3

# CSV WRITER
from csv import writer

with open("01-publications-modified.csv", "w", encoding="utf-8") as f:
    publications_modified = writer(f)
    publications_modified.writerows(publications_list) # the method writerows prende liste python e ti permette di copiarle nel file csv
    # NB!!!!! Writerow se è una lista semplice, Writerows se è una lista di liste

# DictReader and DictWriter
# On python, we can represent the CSV tables as list of D I C T I O N A R I E S 

from csv import DictReader # meglio usare dictreader che solo reader

with open("01-publications-modified.csv", "r", encoding="utf-8") as f:
    publications_modified = DictReader(f)  # it is a reader operating as a list of dictionaries, mentre legge trasforma in dict
    publications_modified_dict = list(publications_modified)  # casting the reader as a list, così possiamo accedere più facilmente
 # accedervi: print(publications_modified_dict[0]["Nome"]) 0 perchè è una lista di dizionari, quindi devi specificare quale lista

pprint(publications_modified_dict) #returns a LIST of DICTIONARIES. Each dictionary= a row
# si possono usare i metodi dei dizionari per accedere alle chiavi, ai valori, eccetera

# dopo aver modificato il file, puoi storarlo nel csv usando dictwriter
from csv import DictWriter

with open("01-publications-modified-dict.csv", "w", encoding="utf-8") as f:
    header = [  # the fields defining the columns must be explicitly specified in the desired order
        "doi", "title", "publication year", "publication venue", "type", "issue", "volume" ]
    
    publications_modified = DictWriter(f, header)
    publications_modified.writeheader()  # the header must be explicitly created in the output file
    publications_modified.writerows(publications_modified_dict)  # it writes all the rows, as usual
   
# NB!!!! Essendo un dizionario non c'è ORDINE
# l'HEADER non è facoltativo qui, deve essere specificato 

# CSV ha dialetti, come Tab-separated Values (TSV).
with open("01-publications-modified-dict.tsv", "w", encoding="utf-8") as f:
    header = [  # the fields defining the columns must be explicitly specified in the desired order
        "doi", "title", "publication year", "publication venue", "type", "issue", "volume" ]
    
    publications_modified = DictWriter(f, header, dialect="excel-tab")  # adding the specific dialect
    publications_modified.writeheader()  # the header must be explicitly created in the output file
    publications_modified.writerows(publications_modified_dict)  # it writes all the rows, as usual


# JSON

from json import dump

with open("01-publications-venues-modified.json", "w", encoding="utf-8") as f:
    dump(json_doc, f, ensure_ascii=False, indent=4)

# nb. per convertire un cvs in jason, è necessario che il file venga convertito in una lista (di dizionari, se usi DictReadeer)
# esempio: 
from csv import DictReader
from json import dump
from pprint import pprint

def csv_to_json(data): 
    with open(data, "r", encoding="utf-8") as f:
        publications_modified = DictReader(f)  # it is a reader operating as a list of dictionaries
        publications_modified_dict = list(publications_modified)  # casting the reader as a list

    pprint(publications_modified_dict)
    with open("01-publications-venues-modified.json", "w", encoding="utf-8") as f:
        dump(publications_modified_dict, f, ensure_ascii=False, indent=4) # prende le liste del dizionario e le mette su un json file

# INTRODUCTION TO PANDAS
# Pandas introduces two new classes of objects that are used to handle data in tabular form. They are the class Series and the class DataFrame.
# SERIES
# class Series= one-dimensional array (i.e. it acts as a list) of objects of any data type (integers, strings, floating point numbers, Python objects, etc.). 
# [...] each item in the series is indexed by a specific label (it can be an integer, a string, etc.), that can be used to access such an item. If no index is specified, the class Series will create an index automatically using non-negative numbers

from pandas import Series

my_series = Series(["Ron", "Hermione", "Harry", "Tom", "James", "Lily", "Severus", "Sirius"])
print(my_series) 
# output  
0         Ron
1    Hermione
2       Harry
3         Tom
4       James
5        Lily
6     Severus
7      Sirius
dtype: object # in Pandas, the object data type (i.e. dtype) is used to define series that are made of string or mixed type objects. Can contain any kind of data!! 

my_series = Series(["Ron", "Hermione", "Harry", "Tom", "James", "Lily", "Severus", "Sirius"], 
                   dtype="string", name="given name") # you can also specify which type should be there in the series and its name
print(my_series)
0         Ron
1    Hermione
2       Harry
3         Tom
4       James
5        Lily
6     Severus
7      Sirius
Name: given name, dtype: string 

# metodi che si possono utilizzare
sub_series = my_series[1:6] #slicing
print("The new subseries is:")
print(sub_series)

print("\nThe element at index 5 of the new subseries is:") # anche get si può usare, oltre a questo con [index]
print(sub_series[5]) # NON è la posizione della serie (altrimenti sarebbe out of range) MA è il VALORE che voglio, che ha come indice 5, non posizione 5 (la posizione in una lista sarebbe 6, perchè includerebbe l'1)

# DATA FRAME 
# class DataFrame=  a DataFrame is a table. You can imagine it as a set (non-repeatable) of named series containing the same amount of elements, where each series defines a column of the table, and all the series share the same index labels 

index 	column name 1 (a series)	column name 2 (another series)
0	                Ron	                      Wisley
1	                Hermione	              Granger

from pandas import DataFrame

my_dataframe = DataFrame({
    "given name" : my_series, # row fatta già prima
    "family name" : Series(
        ["Wisley", "Granger", "Potter", "Riddle", "Potter", "Potter", "Snape", "Black"], dtype="string") # il nome che specifichi è il nome della colonna! 
})

print(my_dataframe)

# ACCEDERE ALLA COLONNA                                                                   ["qualcosa"]
family_name_column = my_dataframe["family name"] # VERTICALE                              # SERIES SERIES SERIES SERIES [0] (.loc)
print(family_name_column) # printa la serie, la colonna family name                       # DATAFRAME                   [1] (.loc)
                                                                                          # DATAFRAME                   [2] (.loc)
# ACCEDERE ALLA ROW
second_row = my_dataframe.loc[1] # devi solo aggiundere .loc # ORIZZONTALE, DATI DELLO STESSO TIPO
print(second_row)

# ritorna comunque una serie
given name      Hermione (index, element)
family name    Granger (index, element)
Name: 1, dtype: string (il nome diventa lo index della row)
print(second_row["family name"]) # Granger

# also data frame can be sliced (by rows) using the indentical approach introduced in the series. For instance, the following code shows how to create a new data frame taking a selection of the rows:

print("The new subdataframe is:")
sub_dataframe = my_dataframe[1:6] # printa le righe fino alle 5
print(sub_dataframe)

print("\nThe row at index 2 of the new subdataframe is:")
print(sub_dataframe.loc[2]) # printa la riga con index 2

# LOADING DATA INTO PANDAS
#The method read_csv takes in input a file path and returns a DataFrame representing such tabular data, as shown as follows:

from pandas import read_csv

# MA pandas deve un attimo capire cosa sono i valori nelle colonnedevi dirgli tu che tipi di data sono

df_publications = read_csv("../01/01-publications.csv", 
                           keep_default_na=False,
                           dtype={
                               "doi": "string",
                               "title": "string",
                               "publication year": "int",
                               "publication venue": "string",
                               "type": "string",
                               "issue": "string",
                               "volume": "string"
                           })

# ITERATING THE DATAFRAME
for idx, row in df_publications.iterrows(): # COLONNE, VERTICALE 

# ITERATING THE SERIES
for column_name, column in df_publications.items(): # ORIZZONTALE

# STORING DATA 
# the method to_csv will store also an additional column at the beginning, i.e. that related with the index labels for each row. In order to avoid to preserve the index, it is possible to set the input named parameter index to False

df_publications.to_csv("03-publications_no_index.csv", index=False)

# OPERATIONS WITH DATA FRAMES
df_publications.query("type == 'journal article'")

# in case we want to refer to columns with spaces, we must use the tick character (i.e. `) to enclose the name of the column. 
 
df_publications.query("`publication year` < 2003") # tutte le rows che hanno il pub. year minore di 2003

# It is also possible to combine queries by using the boolean operators and and or. For instance, to get all the journal articles published before 2003, we can run the following query:
df_publications.query("type == 'journal article' and `publication year` < 2003")

# JOINING
# Joining two data frames into a new one according to some common value. Facciamo come se avessimo importato, come abbiamo fatto prima, un altro file .csv chiamato df_venues

from pandas import merge

df_joined = merge(df_publications, df_venues, left_on="publication venue", right_on="id")
df_joined  # draw the table in the notebook

df_joined.query("type_y == 'journal' and name == 'Current Opinion in Chemical Biology'") # an example of query the merged datasets


# RELATIONAL DATABASES 

# SQLite is a relational database management system (RDBMS) which can be embedded into the end program and does not follow a classic client–server architecture, 
# where the server database is independent and is actually accessed by client programs.
# Python includes SQLite within its standard library: you can create and modify SQLite database directly from Python code. 

from sqlite3 import connect #open the connection to an existin database it exists, if not, it creates one

with connect("publications.db") as con: # simile alla prima handson lecture. Prende come argomento un URL. NB!!! Why using with? Because it automatically closes the connection with the database!!
   # what we usually do with databases is transactions or some operations. Then you do this:
    con.commit() # now all the operations that I did are packed within a transaction and are pushed. 
    # when you commit, the collection is CLOSED, thanks to the "with"
    # su JupyterLab, a questo punto, dovrebbe apparire il file che hai creato da connettere a database. Quindi se ci ritorni,
    # il file teoricamente è già stato creato, come detto precedentemente

    # il prof ci suggerisce un modo per trasformare un data model in un reelational database:
    # 1) Creiamo una tavola per ogni subclasse( NON classe ) e come colonne abbiamo tutti gli attributi che hanno un singolo valore, sia delle stesse subclassi che delle loro classi
    # 2) Per ogni tavola che abbiamo creato aggiungiamo una nuova colonna che specifica un internal id, un unique identifier. 
    # 3) Potremmo dover creare la connessione tra due tavole, perchè in relazione tra loro. Per ex. la colonna del PublicationVenue, dovrebbe contenere gli stessi valori che puoi trovare nella tavola "Venue". Quindi abbiamo due colonne, l'id della classe a cui si riferisce e l'id suo personale
    # 4) Bisogna creare un'altra tavola con due colonne, dove nella prima colonna contien il local id della classe che ha più attributi(fino ad ora abbiamo considerato solo gli attributi che hanno 1 solo valore)

# In questo modo, gli id act like internal keys. Quelli che ionvece interlinkano più tables, act like foreign id.
# VENUE-ID TABLE, prima facciamo l'ultimo passo
from pandas import read_csv, Series

venues = read_csv("../01/01-venues.csv", # file su github 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "name": "string",
                      "type": "string"
                  })

# This will create a new data frame starting from 'venues' one,
# and it will include only the column "id"
venues_ids = venues[["id"]]

# Generate a list of internal identifiers for the venues
venue_internal_id = []
for idx, row in venues_ids.iterrows():
    venue_internal_id.append("venue-" + str(idx))

# Add the list of venues internal identifiers as a new column
# of the data frame via the class 'Series'
venues_ids.insert(0, "venueId", Series(venue_internal_id, dtype="string"))

# Show the new data frame on screen
venues_ids

# TABLES FOR JOURNALS AND BOOKS
# nel file cs, l'ultima colonna ci dice il tipo, quindi dobbiamo escludere le altre. Possiamo farlo con il metodo pandas "query"
journals = venues.query("type == 'journal'")
journals  # Showing the data frame

# Poi, per ogni row abbiamo bisogno del local id, che non abbiamo qui. Ma è nella Venue-id table. Quindi, bisogna joinarle.
from pandas import merge

df_joined = merge(journals, venues_ids, left_on="id", right_on="id") # id hanno lo stesso nome. Quindi pandas le sovrappone, le mette in una sola colonna, perchè hanno gli stessi valori e lo stesso nome di colonna
df_joined 

# possiamo anche rinominare le colonne 
journals = df_joined[["venueId", "name"]]
journals = journals.rename(columns={"venueId": "internalId"})
journals

# Data frame of books, exactly the same approach

books = venues.query("type == 'book'")
df_joined = merge(books, venues_ids, left_on="id", right_on="id")
books = df_joined[["venueId", "name"]]
books = books.rename(columns={"venueId": "internalId"})
books

# TABLES OF PUBLICATIONS adesso creiamo le classi vere e proprie, non le subclassi
publications = read_csv("../01/01-publications.csv", 
                        keep_default_na=False,
                        dtype={
                            "doi": "string",
                            "title": "string",
                            "publication year": "int",
                            "publication venue": "string",
                            "type": "string",
                            "issue": "string",
                            "volume": "string"
                        })

# Create a new column with internal identifiers for each publication
publication_internal_id = []
for idx, row in publications.iterrows():
    publication_internal_id.append("publication-" + str(idx))
publications.insert(0, "internalId", Series(publication_internal_id, dtype="string"))

publications

# questa è la classe che contiene tutto, però ora abbiamo pubblicazioni che sono giornali e pubblicazioni che sono capitoli di libri
# journal articles
journal_articles = publications.query("type == 'journal article'") # prima si fa la query in publications 

df_joined = merge(journal_articles, venues_ids, left_on="publication venue", right_on="id") # perchè mi serve l'id che si trova nell'altgra tavola (venueid)
journal_articles = df_joined[
    ["internalId", "doi", "publication year", "title", "issue", "volume", "venueId"]]
journal_articles = journal_articles.rename(columns={
    "publication year": "publicationYear",
    "venueId": "publicationVenue"}) # metodo rename. Colums ha come input un dizionario. 
journal_articles

# book chapters
book_chapters = publications.query("type == 'book chapter'") # uguale, cambia solo la query
df_joined = merge(book_chapters, venues_ids, left_on="publication venue", right_on="id")
book_chapters = df_joined[
    ["internalId", "doi", "publication year", "title", "venueId"]]
book_chapters = book_chapters.rename(columns={
    "publication year": "publicationYear",
    "venueId": "publicationVenue"})
book_chapters

# adding the tables to the database. LAST STEP!
with connect("publications.db") as con:
    venues_ids.to_sql("VenueId", con, if_exists="replace", index=False)  # ha come input 3 argomenti
    journals.to_sql("Journal", con, if_exists="replace", index=False)
    books.to_sql("Book", con, if_exists="replace", index=False)
    journal_articles.to_sql("JournalArticle", con, if_exists="replace", index=False)
    book_chapters.to_sql("BookChapter", con, if_exists="replace", index=False)

#se noti, to_sql è un metodo che si mette con le tabelle che abbiamo creato e le pusha direttamente sul database di sql :)

# RDF 
# The Resource Description Framework (RDF) is a high-level data model (NOT A LANGUAGE) based on triples subject-predicate-object called statements.
# A resource is an object identified with a URI, that sometimes they can be URL, for ex. un link di Wikidata. 
# A property is a resource used to describe relation between resources, always identified by an IRI. 
# A statement is a triple subject-predicate-object, where the subject is a resource, the predicate is a property, and the object is either a resource or a literal (i.e. a string): 
    # 1) statement with a resource as an object
    <IRI subject> <IRI predicate> <IRI object> .
    # 2) statement with a literal as an object
    <IRI subject> <IRI predicate> "literal value"^^<IRI type of value> .

# Ci sono 3 modi per esempio con la frase di Umberto Eco.
    #<http://www.wikidata.org/entity/Q12807> <http://www.w3.org/2000/01/rdf-schema#label> "Umberto Eco" .# literals CANNOT BE subjects
    #<http://www.wikidata.org/entity/Q172850> <http://www.w3.org/2000/01/rdf-schema#label> "The Name of the Rose" .
    #<http://www.wikidata.org/entity/Q12807> <http://www.wikidata.org/prop/direct/P800> <http://www.wikidata.org/entity/Q172850> .

# The special property of the RDF
    #  http://www.w3.org/1999/02/22-rdf-syntax-ns#type we can use this property to assign the appropriate type of object to the entities. 
    # <http://www.wikidata.org/entity/Q12807> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Person> qui dice che Umberto ha come type Persona. 

# https://schema.org/ è uno schema di Google per descrivere, identificare qualcuno o qualcosa. 

# RDF GRAPH
    # An RDF Graph is a set of RDF statements. Tutti gli statement RDF definiscono una struttura di grafico diretto. 

# Blazegraph, a database for RDF data, vedi come scaricarlo
    # 1. types are the classes. we need to identify the names of all the most concrete classes (e.g. JournalArticle, BookChapter, Journal, Book);
    # 2. ogni attributo è rappresentato da RDF properties 
    # 3.  subjects are always resources of the source class while the objects are resources of the target class
    # 4. all attributes and relations defined in a class are inherited all its subclasses 

# You can choose to reuse existing classes and properties (e.g. as defined in schema.org) or create your own. 

# In Python 
# prima dobbiamo trovare un modo per creare rdf graphs su python, poi mandarlo sul nostro database. 
# pip install rdflib

from rdflib import Graph
my_graph = Graph() # come nei grafici normali

from rdflib import URIRef

# classes of resources
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")

# attributes related to classes
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
name = URIRef("https://schema.org/name")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

# literals
from rdflib import Literal

a_string = Literal("a string with this value") # possiamo usare i datatype di python (stringa, booleani, etc.), RDF li legge
a_number = Literal(42)
a_boolean = Literal(True)

# i dati che voglio inserire sono nel mio csv file, quindi dobbiamo usare pandas 
from pandas import read_csv, Series
from rdflib import RDF

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://comp-data.github.io/res/"

venues = read_csv("../01/01-venues.csv", 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "name": "string",
                      "type": "string"
                  })

venue_internal_id = {}
for idx, row in venues.iterrows():
    local_id = "venue-" + str(idx)
    
    # The shape of the new resources that are venues is
    # 'https://comp-data.github.io/res/venue-<integer>'

    subj = URIRef(base_url + local_id)
    
    # We put the new venue resources created here, to use them
    # when creating publications
    venue_internal_id[row["id"]] = subj #il pfor ha crerato anche un dizionario che ha utilizzato per assegnare degli URLs alle venue 
    
    if row["type"] == "journal":
        # RDF.type is the URIRef already provided by rdflib of the property 
        # 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        my_graph.add((subj, RDF.type, Journal)) # è una tupla di uri ref: soggetto, predicato, oggetto 
    else:
        my_graph.add((subj, RDF.type, Book))
    
    my_graph.add((subj, name, Literal(row["name"])))
    my_graph.add((subj, identifier, Literal(row["id"])))

# aggiungere informazioni 
publications = read_csv("../01/01-publications.csv", 
                        keep_default_na=False,
                        dtype={
                            "doi": "string",
                            "title": "string",
                            "publication year": "int",
                            "publication venue": "string",
                            "type": "string",
                            "issue": "string",
                            "volume": "string"
                        })

for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)
    
    # The shape of the new resources that are publications is
    # 'https://comp-data.github.io/res/publication-<integer>'
    subj = URIRef(base_url + local_id)
    
    if row["type"] == "journal article":
        my_graph.add((subj, RDF.type, JournalArticle))

        # These two statements applies only to journal articles
        my_graph.add((subj, issue, Literal(row["issue"])))
        my_graph.add((subj, volume, Literal(row["volume"])))
    else:
        my_graph.add((subj, RDF.type, BookChapter))
    
    my_graph.add((subj, name, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["doi"])))
    
    my_graph.add((subj, publicationYear, Literal(str(row["publication year"])))) # era un int. Qui è una stringa. Va contro il datamodel che abbiamo costruito, però il prof ha usato come URI (vedi sopra) quel link dove c'è scritto che dovrebbe ritornare una data, quindi una stringa, non un numero. Ma in realtà puoi fare come vuoi. 
    
    # The URL of the related publication venue is taken from the previous
    # dictionary defined when processing the venues
    my_graph.add((subj, publicationVenue, venue_internal_id[row["publication venue"]]))

    # trasferiamo il grafico nel datamodel 
    from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

store = SPARQLUpdateStore() # è un oggetto che acts as a proxy

# The URL of the SPARQL endpoint is the same URL of the Blazegraph
# instance + '/sparql'
endpoint = 'http://127.0.0.1:9999/blazegraph/sparql' # devi sapere l'endpoiint! Tutti i graph databases mettono a disposizione un endpoint di SPARQL. è l'url del blazegraph + sparql

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint)) #l'input è una tupla!!

for triple in my_graph.triples((None, None, None)): # metodo triples ritorna tutte le triple nel mio graph
   store.add(triple)
    
# Once finished, remeber to close the connection
store.close()

# READING DATA FROM SQLITE (relational)

from sqlite3 import connect
from pandas import read_sql # pandas makeas available this method

with connect("../04/publications.db") as con:
    query = "SELECT title FROM JournalArticle"
    df_sql = read_sql(query, con) 

# usiamo with per chiudere la connessione subito dopo, per non occupare memoria

with connect("../04/publications.db") as con:
    query = "SELECT * FROM JournalArticle"
    df_journal_article_sql = read_sql(query, con)

# Show the series of the column 'publicationYear', which as 'dtype'
# specifies 'int64', as expected
df_journal_article_sql["publicationYear"]

# Reading data from Blazegraph

# The function get is called to perform such an operation, and it takes in input three parameters: 
# the URL of the SPARQL endpoint to contact, the query to execute, and a boolean specifying 
# if to contact the SPARQL endpoint using the POST HTTP method) (strongly suggested, otherwise it could not work correctly)

from sparql_dataframe import get # returns a dataframe

endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
query = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <https://schema.org/>

SELECT ?journal_article ?title
WHERE {
    ?journal_article rdf:type schema:ScholarlyArticle .
    ?journal_article schema:name ?title .
}
"""
df_sparql = get(endpoint, query, True)
df_sparql

publication_query = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <https://schema.org/>

SELECT ?internalId ?doi ?publicationYear ?title ?issue ?volume ?publicationVenue
WHERE {
    VALUES ?type {
        schema:ScholarlyArticle
        schema:Chapter
    }
    
    ?internalId rdf:type ?type .
    ?internalId schema:identifier ?doi .
    ?internalId schema:datePublished ?publicationYear .
    ?internalId schema:name ?title .
    ?internalId schema:isPartOf ?publicationVenue .
        
    OPTIONAL { 
        ?internalId schema:issueNumber ?issue .
        ?internalId schema:volumeNumber ?volume .
    }
}
""" #optional: if you aìhave it: in caso ce l'avessi, dammi anche l'issue e il volume 

df_publications_sparql = get(endpoint, publication_query, True)

# ritorna un dataframe, sempre. Ricorda: pandas attribuisce da solo i data types. Quindi il numero di issue
# e di volume dice che sono numeri (floats), non strings. L'abbiamo già visto e modificato. Ma qui abbiamo semplicemente
# le risposte delle query, non possiamo veramente infierire prima di aver printato il dataframe su python, come abbiamo fatto ora
# usiamo il metodo pandas astype()

df_publications_sparql["issue"] = df_publications_sparql["issue"].astype("string")
df_publications_sparql["volume"] = df_publications_sparql["volume"].astype("string")


# se abbiamo come valore NaN (not a number) o Not an object.. insomma non c'è un valore, possiamo fare così:
# you can use the data frame method fillna, which enables one to replace all NaN in the data frame with a value of your choice passed as input:


df_publications_sparql = df_publications_sparql.fillna("") # il prof l'ha riempito con una stringa vuota

# MA pandas cambia il FLOAT in string... quindi... 4.0 diventa "4.0". Dobbiamo cambiare di nuovo
# Il metodo apply che abbiamo usato prima può anche prendere in input funzioni
# quindi scriviamo una funzione che toglie lo 0 dalla stringa ...
def remove_dotzero(s):
    return s.replace(".0", "")
#... e applichiamolo come argomento del metodo appy su quella colonna in particolare

df_publications_sparql["issue"] = df_publications_sparql["issue"].apply(remove_dotzero) # itera da solo su tutti gli el. della colonna
df_publications_sparql["volume"] = df_publications_sparql["volume"].apply(remove_dotzero)

df_publications_sparql

# Python può anche passare funzioni come input di altre funzioni (metodi). E' un oop ma usa anche cose mixate
# abbiamo visto come mergere due tabelle. Ma come mergiamo stesse informazioni ma su databases diversi?
# possiamo concatenare più tavole diverse di diversi database MA devono avere lo stesso nome di colonne e gli stessi datatypes
# se non abbiamo questa situazione, allora la devi fare tu. Lo hai già fatto manualmente, precedentemente. 


from pandas import concat

df_union = concat([df_journal_article_sql, df_publications_sparql], ignore_index=True) # True perchè altrimenti metterebbe gli stessi index (le tabelle hanno index uguali, ovviamente), quidni meglio
# ignorare gli index delle tabelle e pandas, come sempre, crea il proprio index ordinato 

# once you have merged two dataframes, you need to avoid duplications. You use a method that takes as an argument THE NAMES OF THE ROWS. 
# quindi, tu dici i nomi delle row che deve guardare, se hanno lo stesso VALORE, allora le elimina


df_union_no_duplicates = df_union.drop_duplicates(subset=["doi"])
df_union_no_duplicates

# se voglio ordinare le rows per, ad es. anno di pubblicazione, allora uso questo
df_union_no_duplicates_sorted = df_union_no_duplicates.sort_values("publicationYear")
# se vuoi solo 2 info (due colonne) crei un subdataframe solo con quelle colonne così:
df_final = df_union_no_duplicates_sorted[["title", "publicationYear"]]

# HAI TUTTO PER IL PROGETTO

# COSA FARE QUANDO RICEVI UN DATASET?
# understand what its data are about, how they have been organised, what is the type of each column, and whether there are any null object included in it 
# In a previous tutorial, we have used an input parameter specified on the function read_csv (i.e. keep_default_na set to False) to rewrite empty cell values as empty strings (i.e. ""). However, by doing so, we may miss some relevant information

#lavoriamo sulle colonne soprattutto, per capire le differenze, quanti valori sono uguali, se ci sono doppioni, etc.
from pandas import read_csv

publications = read_csv("07-publications.csv")
publications.info()
# other columns have, somewhere, some cell left unspecified - which is reasonable: a book chapter does not have any issue or volume 
publications.describe(include="all") # descrive un po' le statistiche generali di ogni colonna
# n.b. all the statistics about number manipulations (mean, std, min, etc.) do not apply to strings, and thus a NaN is returned 
# ad es. se vediamo che ci sono due titoli uguali, facciamo una query publications.query('title == "Transformation toughening"')

print(publications["publication year"].median()) # la media ci fa capire come è organizzata la distribuzione dei dati NUMERICI
publications["type"].unique() # quali sono i tipi diversi nel dataframe? libri monografie, article, etc.
type_count = publications["type"].value_counts() # metodo series che restituisce il numero di qualcosa nella colonna, ad es. quanti libri, monografia, journals ci sono. DAI VALORI più grandi ai più piccoli, non per index! Per index, vedi dopo

# GRAFICI CON .PLOT!
type_count.plot(kind="bar") # di questi valori poi possiamo fare i grafici!!!! BAR VERTICALE
# un esempio di come puoi usare anche slice sulle colonne
best_venues = publications["publication venue"].value_counts()[:10] # è un metodo SERIES quindi gli indici diventano i titoli...
best_venues.plot(kind="barh") # BAR ORIZZONTALE 
best_venues.plot(kind="pie") # IL PIE CHART t.t BELLISSIMO
best_venues_sorted = best_venues.sort_index() # sortare per index, non valore!
best_venues.plot() # senza argomenti, il grafico default è quello brutto SUGLI ANNI



class CategoryUploadHandler(UploadHandler): # handles JSON files
    def __init__(self):
        super().__init__()  # inherited from the super

    def pushDataToDb(self, json_path: str) -> bool:
        try:
            with open(json_path, "r", encoding="utf-8") as file:
                json_data = load(file)  # list of dicts

            # univoque id  
            all_categories = []
            id_counter = 1  # counter

            for journal in json_data:
                categories = journal.get("categories", [])  # it could be one or more categories 
                for cat in categories:
                    # ex. id: CAT-001, CAT-002, ...
                    internal_id = f"CAT-{id_counter:03d}" # 3 digits, if not three 0
                    id_counter += 1
                    all_categories.append({
                        "internal_id": internal_id,
                        "category_id": cat.get("id"),
                        "quartile": cat.get("quartile"),
                        "journal_identifiers": '; '.join(journal.get("identifiers", []))
                    })
            df = DataFrame(all_categories)

            with connect(self.getDbPathOrUrl()) as conn:
                df.to_sql("Categories", conn, if_exists="replace", index=False)
            return True

        except Exception as e:
            print(f"Error: impossible to upload {e}")
            return False
        
(2) 
class CategoryUploadHandler(UploadHandler): # handles JSON files
    def __init__(self):
        self.internal_id_counter = 0  # Contatore per gli internal_id numerici

    def generate_internal_id(self):
        internal_id = self.internal_id_counter
        self.internal_id_counter += 1 # starts from 1
        return internal_id
    
    def pushDataToDb(self, json_path: str, db_path: str) -> bool: # ! idk the relationship wiht the super
        # Load JSON data
        with open(json_path, 'r') as file:
            data = load(file)

        # Create internal IDs and build DataFrames
        journals = []
        categories = []
        areas = []
        
        # Internal IDs
        journal_id_map = {}  # to map old identifiers to internal IDs that I created
        category_id_map = {}  # same for category
        area_id_map = {}  # same for area
        
        for journal in data:
            # Generate a unique internal ID for the journal
            journal_internal_id = self.generate_internal_id()
            journal_id_map[journal['identifiers']] = journal_internal_id

            # Create a DataFrame for each category in the journal
            for category in journal['categories']:
                # Generate a unique internal ID for the category
                category_internal_id = self.generate_internal_id()
                category_id_map[category['id']] = category_internal_id
                categories.append({
                    'internal_id': category_internal_id,
                    'journal_internal_id': journal_internal_id,
                    'category_id': category['id'],
                    'quartile': category.get('quartile', None)
                })
            # I have done this so more journals can have the same objects ("categories, areas")
            # Create a DataFrame for areas
            for area in journal['areas']:
                # Generate a unique internal ID for the area
                area_internal_id = self.generate_internal_id()
                area_id_map[area] = area_internal_id
                areas.append({
                    'internal_id': area_internal_id,
                    'journal_internal_id': journal_internal_id,
                    'area': area
                })
            
            # Add journal information
            journals.append({
                'internal_id': journal_internal_id,
                'identifiers': journal['identifiers'],
            })
        
        # Step 3: Convert to DataFrames
        journal_df = DataFrame(journals)
        category_df = DataFrame(categories)
        area_df = DataFrame(areas)
        
        # Step 4: Connect to SQLite database and insert data
        try:
            conn = connect(db_path)
            cursor = conn.cursor()
            
            # Create tables if they don't exist
            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Journal (
                    internal_id INTEGER PRIMARY KEY,
                    identifiers TEXT
                )
            ''')

            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Category (
                    internal_id INTEGER PRIMARY KEY,
                    journal_internal_id INTEGER,
                    category_id TEXT,
                    quartile TEXT,
                    FOREIGN KEY (journal_internal_id) REFERENCES Journal(internal_id)
                )
            ''')

            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS Area (
                    internal_id INTEGER PRIMARY KEY,
                    journal_internal_id INTEGER,
                    area TEXT,
                    FOREIGN KEY (journal_internal_id) REFERENCES Journal(internal_id)
                )
            ''')

            # Insert data into the tables
            journal_df.to_sql('Journal', conn, if_exists='append', index=False)
            category_df.to_sql('Category', conn, if_exists='append', index=False)
            area_df.to_sql('Area', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close() # ? maybe with 'with' it is not needed? check
            return True
        except Exception as e:
            print(f"Error uploading data: {e}")
            return False

