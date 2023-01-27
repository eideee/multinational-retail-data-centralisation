# Multinational Retail Data Proejct

## 1 - Set up the environment
The repository was setup on both desktop and GitHub

## 2 - Extract and clean the data from data sources.
The Sales_Data database was created by using PGAdmin.

data_cleaning, data_extraction and database_utility classes were created to facilitate data cleaning, data extraction from various sources and initialisation as well as uploading data into the database.

Data extraction from various sources was succesfully done and converted into dataframe. 
Amongst others, the data sources are from:
1. AWS database where the code need to pass authetication credentials in order to download the data. Credentials from YAML file were parsed and passed to gain access. Utilise SQLalchemy library.
2. PDF document in an AWS S3 bucket by using tabula library. 
3. Retrieve data through API and parse JSON file which later the generated list was converted into dataframe.
4. CSV format in an S3 bucket on AWS.
5. Amazon Relational Database Service (RDS)

## 3 - Create the database schema
Casted the column data type to be the desired configuration. Created new column to have categorical data based on input from another table. Configured the primary and foreign keys on all tables to permit further analysis and database query.

<<<<<<< HEAD
## Milestone 4 - Querying the data.
Conducted various database query to answer the questions. Subqueries and joins as well as CTE (common table expressions) were used extensively througout the exercise.

=======
## 4 - Querying the data.
Conducted various database query to answer the questions. Subqueries and joins as well as CTE (common table expressions) were used extensively througout the exercise.
>>>>>>> 8ab9304 (Updated the READEME file)
