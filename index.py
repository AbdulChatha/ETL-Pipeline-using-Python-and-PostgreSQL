# Import necessary libraries
import pandas as pd  # For data manipulation
from sqlalchemy import create_engine, text  # For database connection and queries
import psycopg2  # For PostgreSQL connection

# Read CSV file into a DataFrame
df = pd.read_csv('Address list.csv')

# Remove rows with missing values
df = df.dropna()

# Define a function to extract house number, city, state code, and zip code from the 'address' column
def extract_address_parts(address):
    """
    Extracts house number, city, state code, and zip code from an address string.

    Parameters:
    address (str): A string containing the full address (e.g., '123 Main St, City, ST 12345 USA').

    Returns:
    pd.Series: A Pandas Series containing the extracted parts: house_no, city, state_code, and zip_code.
    """
    # Split the address into parts based on comma separation
    parts = address.split(', ')
    
    # Extract house number and street name (though street name is not used in the output)
    house_no = parts[0].split(' ')[0]
    street_name = ' '.join(parts[0].split(' ')[1:])
    
    # Extract city
    city = parts[1]
    
    # Extract state code and zip code
    state_code = parts[2].split(' ')[0]
    zip_code = parts[2].split(' ')[1]
    
    # Extract country (though not used in the output)
    country = parts[2].split(' ')[2]
    
    # Return the extracted components as a Pandas Series
    return pd.Series([house_no, city, state_code, zip_code], index=['house_no', 'city', 'state_code', 'zip_code'])

# Apply the function to the 'address' column and create new columns: 'house_no', 'city', 'state_code', 'zip_code'
df[['house_no', 'city', 'state_code', 'zip_code']] = df['address'].apply(extract_address_parts)

# Sort the DataFrame by 'state_code' and 'city' for better organization
df_sorted = df.sort_values(by=['state_code', 'city'])

# Drop unwanted columns: 'id' and 'address'
df_sorted = df_sorted.drop(columns=['id', 'address'])

# Reset the index of the sorted DataFrame
df_sorted = df_sorted.reset_index(drop=True)

# Database connection parameters
DB_NAME = "Addresses"  # Name of the PostgreSQL database
DB_USER = "postgres"  # Username for PostgreSQL
DB_PASSWORD = "123"  # Password for PostgreSQL
DB_HOST = "localhost"  # Host address (localhost for local connection)
DB_PORT = "5432"  # Port number for PostgreSQL

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# SQL query to create the 'addresslist' table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS addresslist (
    id SERIAL PRIMARY KEY, 
    house_no VARCHAR(50),  
    city VARCHAR(100),  
    state_code CHAR(2),  
    zip_code VARCHAR(10), 
    lat DECIMAL(9, 6),  
    lng DECIMAL(9, 6)  
);
'''

# Execute the table creation query
cursor.execute(create_table_query)

# Commit the changes to the database
connection.commit()

# Close the cursor and the database connection
cursor.close()
connection.close()

# Create a SQLAlchemy engine for use with Pandas
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Insert the sorted DataFrame into the 'addresslist' table in PostgreSQL
df_sorted.to_sql('addresslist', engine, if_exists='append', index=False)

# Query to fetch the count of addresses grouped by 'state_code'
with engine.connect() as conn:
    query = text("""
    SELECT state_code, COUNT(*) AS address_count
    FROM addresslist
    GROUP BY state_code;
    """)
    
    # Execute the query and fetch all results
    result = conn.execute(query)
    rows = result.fetchall()

# Convert the query results to a DataFrame for better readability
df = pd.DataFrame(rows, columns=['state_code', 'address_count'])

# Export the results to a CSV file
df.to_csv("address_count_by_state_code.csv")
