import requests
from duckduckgo_search import DDGS

from sqlalchemy import create_engine
from sqlalchemy import text

db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'db'
db_port = '5432'

# Connect to the database
db_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)


def add_new_row(n):
    # Insert a new company into the 'companies' table.
    with db.connect() as conn:
        params = ({"name": n["name"], "company_id": n["company_id"], "url": n["url"], "version": n["version"]})
        conn.execute(text("INSERT INTO companies (name,company_id,url,version) VALUES (:name, :company_id, :url, :version)"), params)
        conn.commit()
        print('The last value inserted is: {}'.format(n["name"]))


def get_company_website(company_name, company_contact_details):
    # Go through contact details and try to get the website url.
    type_keys = ['Kotisivun www-osoite', 'www-adress', 'Website address']
    for company_contact_detail in company_contact_details:
        if company_contact_detail['type'] in type_keys:
            return company_contact_detail['value']

    # If website does not exist in the API data,
    # attempt to get it from DuckDuckGo.
    with DDGS(timeout=50) as ddgs:
        results = [r for r in ddgs.text(company_name, region='fi-fi', max_results=1)]
        # Get the first result and return the url.
        return results[0]['href']


def get_companies():
    # Get companies.
    print('Requesting companies from the API')
    parameters = {
        "totalResults": "false",
        "maxResults": 70,
        "resultsFrom": 70,
        "companyRegistrationFrom": "2000-01-01",
        "companyForm": "OYJ",
    }
    response = requests.get('https://avoindata.prh.fi/bis/v1', params=parameters)
    if response.status_code != 200:
        print('PRH request failed: {}'.format(response.status_code))
        exit()

    # Go through companies and get the needed data.
    results = response.json()['results']
    for result in results:
        company_name = result['name']
        company_id = result['businessId']
        more_info_url = result['detailsUri']

        if not company_name:
            print('No name for company: {}'.format(company_id))
            continue

        # Get detailed info of company.
        company_more_info_endpoint = 'https://avoindata.prh.fi/bis/v1/{}'.format(company_id)
        company_response = requests.get(company_more_info_endpoint)
        if company_response.status_code != 200:
            print('PRH request for detailed info failed: {}'.format(response.status_code))
            continue

        contact_details = company_response.json()['results'][0]['contactDetails']
        company_website = get_company_website(company_name, contact_details)

        # Save data to db.
        company = {
            "name": company_name,
            "company_id": company_id,
            "url": company_website,
            "version": "test"
        }
        add_new_row(company)