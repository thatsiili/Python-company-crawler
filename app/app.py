import time
import random

from sqlalchemy import create_engine
from sqlalchemy import text

db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'db'
db_port = '5432'

# Connecto to the database
db_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)


def add_new_row(n):
    # Insert a new number into the 'numbers' table.
    with db.connect() as conn:
        params = ({"name": n["name"], "company_id": n["company_id"], "url": n["url"], "version": n["version"]})
        conn.execute(text("INSERT INTO companies (name,company_id,url,version) VALUES (:name, :company_id, :url, :version)"), params)
        conn.commit()
        print('The last value inserted is: {}'.format(n["name"]))


if __name__ == '__main__':
    print('Application started')

    # Define values.
    company = {
        "name": "firma",
        "company_id": "123466-5",
        "url": "www.google.fi",
        "version": "Drupal"
    }
    add_new_row(company)
