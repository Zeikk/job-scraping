from config import config, urls
from pymongo import MongoClient
import logging, requests, datetime
from bs4 import BeautifulSoup

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('ETL')

def connection_mongo(user, pwd, db):
    logger.info("Connection BDD : {}".format(db))
    client = MongoClient("mongodb+srv://{}:{}@cluster0.3xvut.mongodb.net/?retryWrites=true&w=majority".format(user, pwd))
    return client, client[db]

def extract_jobs_from_indeed(soup):
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
            job['source'] = 'INDEED'
            jobs.append(job)

    return(jobs)

def extract_jobs_from_linkedin(soup):
    jobs = []
    for ul in soup.find_all(name="ul", attrs={"class":"jobs-search__results-list"}):
        for li in ul.find_all(name="li"):
            job = dict()

            for a in li.find_all(name="a", attrs={"class": "base-card__full-link"}):
                job['href'] =  a['href']

            for title in li.find_all(name="h3", attrs={"class": "base-search-card__title"}):
                job['title'] = title.text.strip()

            for a in li.find_all(name="a", attrs={"class": "hidden-nested-link"}):
                job['company'] = a.text.strip()

            for span in li.find_all(name="span", attrs={"class": "job-search-card__location"}):
                job['location'] = span.text.strip()

            job['date'] = datetime.datetime.today()
            job['source'] = 'LINKEDIN'
            job['_id'] = hash(job['href'])
            jobs.append(job)

    return jobs

def insert_into_mongo(db, jobs, collection):

    nInserted = 0
    for job in jobs:
        if db[collection].count_documents({"_id": job['_id']}) == 0:
            res = db[collection].insert_one(job)
            nInserted += 1

    logger.info("Rows inserted : {} ({})".format(nInserted, collection))

if __name__ == '__main__':

    try:
        logger.info('START ETL')
        client_data, db_data = connection_mongo(config['DB_USER'], config['DB_PWD'], 'data')
        client_web, db_web = connection_mongo(config['DB_USER'], config['DB_PWD'], 'web')

        for object in urls:
            jobs = []
            logger.info('Extraction {} pour les jobs du type {}'.format(object['source'], object['theme']))

            page = requests.get(object['url'])
            soup = BeautifulSoup(page.text, "html.parser")

            # extraction des jobs
            if object['source'] == "LINKEDIN":
                jobs = extract_jobs_from_linkedin(soup)
            elif object['source'] == "INDEED":
                jobs = extract_jobs_from_indeed(soup)

            # insertion dans la base
            if object['theme'] == "WEB":
                insert_into_mongo(db_web, jobs, "jobs")
            elif object['theme'] == "DATA":
                insert_into_mongo(db_data, jobs, "jobs")

        client_data.close()
        client_web.close()

        logger.info("FIN ETL")

    except Exception as err:
        logger.error(err)
