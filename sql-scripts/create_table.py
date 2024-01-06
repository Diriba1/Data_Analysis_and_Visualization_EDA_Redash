import os
import psycopg2
from psycopg2 import sql
import csv

# PostgreSQL connection parameters
connection_params = {
    'host': '192.168.100.30',
    'port': 15433,  # Change this to your specific port number
    'database': 'test_database',
    'user': 'postgres',
    'password': 'postgres',
}

# Root directory containing folders with CSV files
root_directory = 'C:/Users/Diriba/Desktop/10AC/Week3/Week-3-20240102T112054Z-001/Week-3/Week-3-Technical Content/Data/youtube-data'

# Connect to the PostgreSQL database
connection = psycopg2.connect(**connection_params)
cursor = connection.cursor()

# Loop through each folder in the root directory
for folder in os.listdir(root_directory):
    folder_path = os.path.join(root_directory, folder)
    if os.path.isdir(folder_path):
        # Loop through each CSV file in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                table_name = f'{folder}_{filename.split(".")[0]}'

                # Create table based on CSV file header
                with open(os.path.join(folder_path, filename), 'r') as file:
                    csv_reader = csv.reader(file)
                    header = next(csv_reader)
                    columns = [sql.Identifier(col) for col in header]
                    create_table_query = sql.SQL('CREATE TABLE {} ({})').format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(columns + [sql.SQL('TEXT') for _ in columns])
                    )
                    cursor.execute(create_table_query)

                # Upload data to the table, skipping the first line (header)
                with open(os.path.join(folder_path, filename), 'r') as file:
                    next(file)  # Skip the header
                    copy_query = sql.SQL('COPY {} FROM STDIN WITH CSV HEADER DELIMITER %s').format(sql.Identifier(table_name))
                    cursor.copy_expert(copy_query, file, sep=',')

                connection.commit()

# Close the database connection
cursor.close()
connection.close()
