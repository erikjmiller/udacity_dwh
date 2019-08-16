import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to iterate through our staging load queries and execute them.
    Queries pull data from s3 json files and save them into the staging tables.

    :param cur: the database cursor
    :param conn: the database connection
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function to iterate through our star schema queries and execute them.
    Queries pull information out of the staging tables and place them in a more efficient star schema.

    :param cur: the database cursor
    :param conn: the database connection
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Function to performs a load into staging from s3 and another etl from the staging tables into our final star schema.

    :return: None
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