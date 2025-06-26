# multiexcel-project

## Project Overview:

Creating a prototype for reading python files and deduce the schema for saving the data in the database

## Prerequisites(dependencies)

- openpyxl
- pandas
- python
- json
- sqlalchemy

## Steps

- Read the excel
- The excel contains multiple sheets
- Create a dictionary from the read excel
- From the dictionary identify the schema
- Create the database schema ( assuming currently I can support only unstructured data )

## Future steps

- once the schema detection works this can be added to a pipeline to process multiples excel file fed as a binary stream
- Data Cleaning should also be taken care of
- Data visualization capabilites should be added
- ML inference engine to be developed
