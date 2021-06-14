this is our project for the M2 submission of database 


### How it works 

the project conatins a grid index used to run select queries whenever possible alternativly using a linear scan while preserving memory 
access by loading and deloading components from memory.

the gridindex uses 1D vector to simulate a N Dimensional vector by tranlating tuples of coordinates into a singleton coordinate 

while choosing indcies to run the query on a permutation is generated from the list of SQLterms of the query, arranging them from longest length to shortest and then checking if there is an available index for this permutation.

### testing the project 

to test the project go to DBApp.java file in src/java and create a main method or edit whats in it, follow the signature of the methods available in the class and you should be good to go the project supports adding creating tables inserting, deleting, updating, selecting columns and creating Grid indices on and column(s) and using them adaptivly in running the query 