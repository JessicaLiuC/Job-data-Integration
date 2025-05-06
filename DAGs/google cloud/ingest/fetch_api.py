"""
Google Cloud Function to collect job data from multiple APIs
"""
import os
import json
import logging
import datetime
from google.cloud import storage
from muse_api import MuseConnector
from adzuna_api import AdzunaConnector
from jooble_api import JoobleConnector


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


muse_api_key = os.environ.get('MUSE_API_KEY')
adzuna_api_id = os.environ.get('ADZUNA_APP_ID')
adzuna_api_key = os.environ.get('ADZUNA_APP_KEY')
jooble_api_key = os.environ.get('JOOBLE_API_KEY')
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = f"job-data-{PROJECT_ID}"

def upload_to_gcs(data, filename):
    """Upload data to Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
        logger.info(f"File {filename} uploaded to {BUCKET_NAME}")
        return True
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        return False


def collect_jobs(event=None, context=None):
 
    
    # Adzuna API
    try:
        app_id = os.environ.get("ADZUNA_APP_ID")
        app_key = os.environ.get("ADZUNA_APP_KEY")
        
        if app_id and app_key:
            adzuna = AdzunaConnector(app_id, app_key)
            keywords = ["software","data","devops","engineer","IT","developer","designer","manager"]
            adzuna_jobs = adzuna.extract_jobs(keywords=keywords)
            upload_to_gcs(adzuna_jobs, f"adzuna_jobs.json")
        else:
            logger.error("Missing Adzuna API credentials")
    except Exception as e:
        logger.error(f"Error collecting Adzuna jobs: {str(e)}")
    
    # Jooble API
    try:
        jooble_api_key = os.environ.get("JOOBLE_API_KEY")
        
        if jooble_api_key:
            jooble = JoobleConnector(jooble_api_key)
            jooble_jobs = jooble.extract_jobs(
                keywords=["engineer","designer"], 
                locations=["remote"], 
                limit=100
            )
            upload_to_gcs(jooble_jobs, f"jooble_jobs.json")
        else:
            logger.error("Missing Jooble API key")
    except Exception as e:
        logger.error(f"Error collecting Jooble jobs: {str(e)}")
    
    # Muse API
    try:
        muse_api_key = os.environ.get("MUSE_API_KEY")
        
        if muse_api_key:
            muse = MuseConnector(muse_api_key)
            categories = ["ux","design","management","ui","product","interaction","engineer"]
            muse_jobs = muse.extract_jobs(categories=categories)
            upload_to_gcs(muse_jobs, f"muse_jobs.json")
        else:
            logger.error("Missing Muse API key")
    except Exception as e:
        logger.error(f"Error collecting Muse jobs: {str(e)}")
    
    return "Job collection completed"


if __name__ == "__main__":
    collect_jobs()