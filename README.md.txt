This Python script is a command-line tool that allows you to interact with an SQLite database using SQLAlchemy. It supports basic SQL operations such as:

	SELECT * from a chosen table

	SELECT with row limit

	UPDATE values in a specific column

	DELETE rows based on column value

Executing custom SQL queries entered by the user

******************************************************************************

How it works:

Table definitions:
Two tables are created if they do not exist:

stations (e.g. weather station data)

measure (e.g. temperature, precipitation data)

******************************************************************************

Data import:
Data is automatically imported from two CSV files:

clean_stations.csv → into the stations table

clean_measure.csv → into the measure table

******************************************************************************

User interaction:
After the database is prepared, the user is prompted to choose a SQL operation from the menu. Based on the selected option, the program will:

	Display all rows from a table

	Display a limited number of rows

	Update a specific value in a column

	Delete a row based on a value

	Execute a full custom SQL query


Looped interface:
The program runs in a loop until the user chooses to exit (option 0), allowing multiple operations in a single session.