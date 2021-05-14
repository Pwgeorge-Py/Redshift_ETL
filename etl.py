import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function runs all COPY table queries created in 'sql_queries.py'
    These queries move data from the source dataset in S3 into staging tables in redshift
    
    Input:
    cur = cursor object created through psycopg2 module
    conn = connection object created through psycopg2 module
    Output: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function runs all INSERT table queries created in 'sql_queries.py'
    These queries move data from the staging tables into the fact/dimension tables in redshift
    
    Input:
    cur = cursor object created through psycopg2 module
    conn = connection object created through psycopg2 module
    Output: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function: 
    Takes AWS credentials from 'dwh.cfg'
    Creates cursor and connection objects to connect to redshift DB
    Executes function to Load data into staging tables
    Executes function to move data to fact/dimension tables from the staging tables
    
    Input:  None
    Output: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()