import os

from dotenv import  load_dotenv

load_dotenv()

config = {
    "DB_USER": os.environ['DB_USER'],
    "DB_PWD": os.environ['DB_PWD'],
}


urls = [
    {
        'theme': 'WEB',
        'source': 'LINKEDIN',
        'url': 'https://www.linkedin.com/jobs/search?keywords=Web&location=Caen%20et%20p%C3%A9riph%C3%A9rie&geoId=90009697&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0'
    },
    {
        'theme': 'WEB',
        'source': 'INDEED',
        'url': 'https://fr.indeed.com/jobs?q=Web&l=Caen%20(14)&fromage=1'
    },
    {
        'theme': 'DATA',
        'source': 'LINKEDIN',
        'url': 'https://www.linkedin.com/jobs/search?keywords=Big%20data&location=Caen%20et%20p%C3%A9riph%C3%A9rie&geoId=90009697&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0'
    },
    {
        'theme': 'DATA',
        'source': 'INDEED',
        'url': 'https://fr.indeed.com/jobs?q=Data&l=Caen%20(14)&fromage=1'
    }
]