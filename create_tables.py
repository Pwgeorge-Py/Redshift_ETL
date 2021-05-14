import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function runs all DROP table SQL queries created in 'sql_queries.py'
    
    Input:
    cur = cursor object created through psycopg2 module
    conn = connection object created through psycopg2 module
    Output: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function runs all CREATE table SQL queries created in 'sql_queries.py'
    
    Input:
    cur = cursor object created through psycopg2 module
    conn = connection object created through psycopg2 module
    Output: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function: 
    Takes AWS credentials from 'dwh.cfg'
    Creates cursor and connection objects to connect to redshift DB
    Executes functions to DROP and then CREATE all tables in the DB
    
    Input:  None
    Output: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()