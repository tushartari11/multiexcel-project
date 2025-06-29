# multiexcel-project

## Project Overview:

Creating a prototype for reading python files and deduce the schema for saving the data in the database

## Prerequisites(dependencies)

- openpyxl
- pandas
- python
- json
- sqlalchemy

## Working Approach

- Read the excel
- The excel contains multiple sheets
- Create a dictionary from the read excel
- From the dictionary identify the schema
- Create the database schema ( assuming currently I can support only unstructured data )

## Steps to run the project

## Steps to Run the Application

1: Clone the repository

```shell

git clone https://github.com/tushartari11/multiexcel-project.git
cd multiexcel-project
```

2: Run the database using Docker Compose

```shell
docker-compose up -d
```

3: Run the script to read the excel and write records to the db

```shell
python excel_to_database.py
```

4: Check the database records using your favourite db client for postgresql
Below are the parameters to connect

```shell
hostname="localhost"
port=5432
username="admin"
password="admin"
database="mydb"
```

## Future steps

- once the schema detection works this can be added to a pipeline to process multiples excel file fed as a binary stream
- Data Cleaning should also be taken care of
- Data visualization capabilites should be added
- ML inference engine to be developed
