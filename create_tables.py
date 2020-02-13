import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

conn = psycopg2.connect(dbname='dev',
                        host='redshift-cluster.cgzbsoqrtzcd.us-east-2.redshift.amazonaws.com',
                        user='awsuser',
                        password='WestLoop3102',
                        port=5439)


def drop_tables(cur, conn):
    '''
    Purpose:  Drop existing SQL tables
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''
    Purpose:  Create SQL tables using sql statements in sql_queries.py
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    '''
    Purpose: Execute functions
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = conn #psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()