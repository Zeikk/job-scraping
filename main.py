from config import config
from pymongo import MongoClient
import logging, requests, datetime
from bs4 import BeautifulSoup

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def connection_mongo(user, pwd, db):
    logging.info("Connection BDD : {}".format(db))
    client = MongoClient("mongodb+srv://{}:{}@cluster0.3xvut.mongodb.net/?retryWrites=true&w=majority".format(user, pwd))
    return client, client[db]

def extract_job_from_result(soup):
    jobs = []
    for div in soup.find_all(name="ul", attrs={"class":"jobsearch-ResultsList"}):
        for td in div.find_all(name="td", attrs={"class": "resultContent"}):
            print(td)
            job = dict()

            for a in td.find_all(name="a", attrs={"class":"jcs-JobTitle"}):
                for span in a.find_all(name="span"):
                    job['_id'] = a['id'].replace('job_', '').replace('sj_', '')
                    job['title'] = span['title']
                    job['href'] =  a['href']

            for span in td.find_all(name="span", attrs={"class": "companyName"}):
                a = td.find_all(name="a", attrs={"data-tn-element": "companyName"})
                if(len(a) == 0):
                    job['company'] = span.text.strip()
                else:
                    job['company'] = a[0].text.strip()

            job['date'] = datetime.datetime.today()

            jobs.append(job)

    return(jobs)

def insert_into_mongo(db, jobs, collection):

    nInserted = 0
    for job in jobs:
        if db[collection].count_documents({"_id": job['_id']}) == 0:
            res = db[collection].insert_one(job)
            nInserted += 1

    logging.info("Rows inserted : {} ({})".format(nInserted, collection))

if __name__ == '__main__':

    try:
        client_data, db_data = connection_mongo(config['DB_USER'], config['DB_PWD'], 'data')
        client_web, db_web = connection_mongo(config['DB_USER'], config['DB_PWD'], 'web')

        URL = "https://fr.indeed.com/jobs?q=Data&l=Caen%20(14)&fromage=1"
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, "html.parser")

        jobs_data = extract_job_from_result(soup)
        insert_into_mongo(db_data, jobs_data, "jobs")

        URL = "https://fr.indeed.com/jobs?q=Web&l=Caen%20(14)&fromage=1"
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, "html.parser")

        jobs_web = extract_job_from_result(soup)
        insert_into_mongo(db_web, jobs_web, "jobs")

        client_data.close()
        client_web.close()

        logging.info("FIN ETL")

    except Exception as err:
        logging.error(err)
