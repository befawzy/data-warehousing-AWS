import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function takes a list of drop table queries, and the established connection to AWS, i.e. Redshift.
    Then, it executes the queries"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function takes a list of create table queries, and the established connection to AWS, i.e. Redshift.
    Then, it executes the queries"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.ini")

    HOST = config.get("CLUSTER", "HOST")
    DB = config.get("CLUSTER", "DB")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    PORT = config.get("CLUSTER", "PORT")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            HOST, DB, DB_USER, DB_PASSWORD, PORT
        )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
