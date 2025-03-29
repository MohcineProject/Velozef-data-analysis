# myapp/views.py
from django.shortcuts import render
from cassandra.cluster import Cluster

def get_cassandra_data():
    # Connect to the Cassandra cluster, return None if connection failed
    try : 
        cluster = Cluster(['cassandra1' , 'cassandra2']) 
        session = cluster.connect('velozef')  
    except Exception  : 
        return None 

    # List of tables in the keyspace
    tables = ["station_information", "station_status_windowed", "most_charged_bikes" , "bike_situation" , "bike_status" , "batch_station_status"]  

    # Prepare a dictionary to hold column names and rows for each table
    data = {}

    for table in tables:
        # Get the column names for the current table
        rows = session.execute(f"SELECT * FROM {table} LIMIT 30")  # Fetch the last 30 rows
        columns = rows.column_names

        # Store the columns and rows in the data dictionary
        data[table] = {'columns': columns, 'rows': rows}

    return data

def home(request):
    # Get the Cassandra data
    data = get_cassandra_data()

    # Check if data is None
    if data is None : 
        return render(request, 'exceptions/cassandra_not_available.html')

    # Render the page with the data
    return render(request, 'main/home.html', {'data': data})
