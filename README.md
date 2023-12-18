# World-covid
The data is provided by "ourworldindata.org".
This project was done by python through making connection with postgresql for automating the process of making all possible visuals from the data.
The project consists of a few steps:
1. cleaning the data (DataCleaning.py)
2. Creating tables of data in postgresql and inserting the cleaned data in it (PostgresTableCreating.py)
4. Creating a few different materialized views for the purpose of making the analysis faster (PostgresMaterializedView.py)
5. writting all needed function in a separate file to be used in analysis (AnalysisFunctions.py)
6. Creating the file to run the process of creating visuals 

# Note:
1. The dataset is too large (cannot be updated here) check the above mentioned website and search for covid dataset to find the data.
2. The first four stages should be done independently
3. The project was run in Ubuntu OS, thus if you use a different OS, it is necessary to make some changes including the file's location addresses in the first four steps
