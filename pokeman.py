

import psycopg2
import pandas as pd


def create_database():
    # connect to default database
    # conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=123success")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS poke")
    cur.execute("CREATE DATABASE poke")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=poke user=postgres password=123success")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# Extract and Read the data
pokeman = pd.read_csv('/Users/ghost/Downloads/pokemon_data.csv')


# Read the data
pokeman.head()

# Transform the data
pokeman_clean = pokeman[['Name', 'Type 1', 'Type 2', 'HP',
                         'Attack', 'Defense', 'Sp. Atk', 'Sp. Def', 'Speed']]


pokeman_clean.head()


# Sum the data
pokeman['Total'] = pokeman['HP'] + pokeman['Attack'] + pokeman['Defense'] + \
    pokeman['Sp. Atk'] + pokeman['Sp. Def'] + pokeman['Speed']


# Rearrange the data
pokeman['Total'] = pokeman.iloc[:, 4:10].sum(axis=1)


# Rename the column title
pokeman.rename({'#': 'id_code', 'Type 1': 'Type_1', 'Type 2': 'Type_2',
               'Sp. Atk': 'Sp_Atk', 'Sp. Def': 'Sp_Def'}, axis=1, inplace=True)


pokeman.head()


cols = list(pokeman.columns)
#pokeman= pokeman[cols[0:4] + [cols[-1]] + cols[4:10]]
pokeman_clean = pokeman[cols[0:9] + [cols[-1]]]

pokeman_clean.head()


cur, conn = create_database()


# In[ ]:


#pokeman_clean = pokeman_clean.str.replace(' ', '')
# pokeman_clean = pokeman_clean.str.replace('#', '')


# Create a table

poke_table_create = (""" CREATE TABLE IF NOT EXISTS pokeman_clean(id_code numeric PRIMARY KEY,
Name VARCHAR,
Type_1 VARCHAR,
Type_2 VARCHAR,
HP numeric,
Attack numeric,
Defense numeric,
Sp_Atk numeric,
Sp_Def numeric,
Total numeric
)""")


# Execute the table created

cur.execute(poke_table_create)
conn.commit()


# Insert the value into the table
poke_table_create_insert = (""" INSERT INTO pokeman_clean(
id_code,
Name,
Type_1,
Type_2,
HP,
Attack,
Defense,
Sp_Atk,
Sp_Def,
Total)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")


# loop through the data and insert the values into each column
for i, row in pokeman_clean.iterrows():
    # print(list(row))
    cur.execute(poke_table_create_insert, list(row))

conn.commit()
