# Nico's branch
Hello!

## Notes from 5/4/25

To give ourselves a better reference point of how each class (and subclass) works with eachother, I have copied and pasted a bit of code from the testing file. See the 'Uses of the classes' heading in the GitHub page on the project's information. I want you to keep this in mind when looking at everything else that I am explaining.

```py3
rel_path = "relational.db"
cat = CategoryUploadHandler() # this happens FIRST
cat.setDbPathOrUrl(rel_path)
cat.pushDataToDb("data/scimago.json")

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
jou = JournalUploadHandler() 
jou.setDbPathOrUrl(grp_endpoint)
jou.pushDataToDb("data/doaj.csv")
```

### The Category and Journal upload handlers (and the `UploadHandler` superclass)
These can be stubs for now unless there is a more meaningful use for them. They are merely a good way to 'type' the object passed into the `pushDataToDb` method and to determine whether the processing applied is for the CSV data or the JSON data. Think about it like this: is the object that you have created a `JournalUploadHandler` or a `CategoryUploadHandler`? 

P.S: The `path` parameter here is the string name for the CSV/JSON files that the data comes from. It is not the same as the path that is used in the `Handler` class. Ilaria and I will work on the related JSON data processing here (with Pandas), while the CSV side of things should be done by Martina and Rumana. I will suggest a certain implementation of the function with some steps that resemble pseudocode:

```py3
# IF the object is a CategoryUploadHandler (or maybe if the path name ends with .json)
  # A bunch of JSON-related processing should be done with Pandas
  # Once this is done well, each dataframe can be pushed to the RELATIONAL database specified (i.e. the relational database path)
  # The function returns True, indicating that this process has been successful
# ELSE IF the object is a JournalUploadHandler (or maybe if the path name ends with .csv)
  # A bunch of CSV-related processing should be done with Pandas
  # Once this is done well, each dataframe can be pushed to the GRAPH database specified (i.e. the graph database URL)
  # The function returns True, indicating that this process has been successful
# ELSE (in the case that neither condition is met)
  # The function returns False, indicatng that this process has not been successful
```


### The `Handler` class
1. The `pathOrUrl` string parameter of the setter method `setDbPathOrUrl()` will always be a string; however, this string depends on which kind of database is being initialised. In the case that CSV data is being worked with, the value will be the string of the URL where the Blazegraph database is contained (i.e. in the `grp_endpoint` variable). However, in the JSON case, the value passed in will be the string name of the SQLite database.

2. There should be three `if` statements, each dealing with different cases. The order of the first two does not matter, but I will present it in this way regardless. Before the data is pushed to the database in the `pushDataToDb()` method found in the `CategoryHandler` class, the path to the database should be set, and the function should return `True` or `False` depending on whether this process is successful or not. I assume this is for testing/debugging purposes, though I am not exactly sure. The first `if` statement should handle the case where a URL is passed in the function and check that it is a valid URL. I think that the best way to do this would be to use regular expressions (regex) to ensure that the format of the link is valid. The second `if` statement should handle where a path (i.e. a path to a database stored locally) is the value that is passed in the function. Ilaria came up with a good idea here, suggesting that the `.endswith()` function should be used, checking if the end of the string is '.db'. In the case that the string passed in does not meet either of these conditions, the function should return `False` to indicate that the process has not been successful (again, I think this is for testing reasons).

### Other possibilities

These are the two likely cases
1. The CategoryUploadHander is responsible for manipulating the data and preparing it so that it can be pushed to the database
* Data manipulation is done solely with JSON/CSV
2. The CategoryUploadHandler is merely an object that is used to identify that the processes are operating on a JSON file
* This is closer to what we have done in class
* The `.isinstance()` method can be used to check if the object is an instance of either the category/journal upload handler classes
* Data manipulation is done with Pandas (through importing a JSON/CSV file)

### Priorities
Before Monday, please aim to get your part of the `UploadHandler` done (since I am inclined to believe that this is where the data processing happens). As mentioned, this is more faithful to what Peroni has taught us, so I imagine that it is the way to go here.


