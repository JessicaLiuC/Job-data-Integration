import os
import json
import logging
import pandas as pd
import tempfile
from google.cloud import storage
from io import StringIO

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = f"job-data-{PROJECT_ID}"

def download_json_from_gcs(bucket_name, source_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        if not blob.exists():
            logger.error(f"File {source_blob_name} not found in bucket {bucket_name}")
            return pd.DataFrame()
            
        json_content = blob.download_as_text()
        df = pd.read_json(StringIO(json_content))
        logger.info(f"Successfully downloaded and parsed {source_blob_name}")
        return df
    except Exception as e:
        logger.error(f"Error downloading {source_blob_name}: {str(e)}")
        return pd.DataFrame()

def upload_to_gcs(data, destination_blob_name, bucket_name=BUCKET_NAME):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        if isinstance(data, pd.DataFrame):
            json_data = data.to_json(orient='records', indent=4)
            blob.upload_from_string(json_data, content_type="application/json")
        else:
            blob.upload_from_string(data, content_type="application/json")

        logger.info(f"File {destination_blob_name} uploaded to {bucket_name}")
        
        if blob.exists():
            logger.info(f"Verified upload of {destination_blob_name}")
            return True
        else:
            logger.error(f"Upload verification failed for {destination_blob_name}")
            return False
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        return False

def transform_job_data(input_prefix="", output_prefix="transformed_", bucket_name=BUCKET_NAME):
    logger.info("Starting job data transformation")
    
    adzuna_source = f"{input_prefix}adzuna_jobs.json"
    jooble_source = f"{input_prefix}jooble_jobs.json"
    muse_source = f"{input_prefix}muse_jobs.json"
    
    df_adzuna = download_json_from_gcs(bucket_name, adzuna_source)
    df_jooble = download_json_from_gcs(bucket_name, jooble_source)
    df_muse = download_json_from_gcs(bucket_name, muse_source)
    
    if df_adzuna.empty and df_jooble.empty and df_muse.empty:
        logger.error("No data found in source files")
        return None
    
    logger.info(f"Downloaded Adzuna records: {len(df_adzuna)}")
    logger.info(f"Downloaded Jooble records: {len(df_jooble)}")
    logger.info(f"Downloaded Muse records: {len(df_muse)}")
    

    field_mappings = {
        'df_adzuna': {
            'job_title': 'title',
            'job_description': 'description',
            'job_url': 'redirect_url',
            'posted_date': 'created',
            'job_category': 'category.label',
            'job_type': 'contract_time',
            'company_name': 'company.display_name',
            'salary': 'combined_salary_string',   
            'salary_min': 'salary_min',
            'salary_max': 'salary_max',
            'source': 'adzuna' 
        },
        'df_jooble': {
            'job_title': 'title',
            'job_description': 'snippet',
            'job_url': 'link',
            'posted_date': 'updated',
            'job_category': 'type',
            'job_type': 'type',
            'company_name': 'company',
            'salary': 'salary',
            'source': 'jooble'  
        },
        'df_muse': {
            'job_title': 'name',
            'job_description': 'contents',
            'job_url': 'refs.landing_page',
            'posted_date': 'publication_date',
            'job_category': 'categories[0].name', 
            'job_type': '',   
            'company_name': 'company.name',
            'salary': '',
            'source': 'muse' 
        }
    }

    df_standardized_adzuna = pd.DataFrame()
    df_standardized_jooble = pd.DataFrame()
    df_standardized_muse = pd.DataFrame()
    
    if not df_adzuna.empty:
        mapping_adzuna = field_mappings['df_adzuna']
        df_standardized_adzuna['source'] = 'adzuna'
        
        for new_col, original_col in mapping_adzuna.items():
            if new_col not in ['salary_min', 'salary_max', 'source']:
                if '.' in original_col:
                    parts = original_col.split('.')
                    if parts[0] in df_adzuna.columns:
                        df_standardized_adzuna[new_col] = df_adzuna[parts[0]].apply(
                            lambda x: x.get(parts[1]) if isinstance(x, dict) and parts[1] in x else None
                        )
                else:
                    if original_col in df_adzuna.columns:
                        df_standardized_adzuna[new_col] = df_adzuna[original_col]
                    else:
                        df_standardized_adzuna[new_col] = None
        
        # Handle salary separately
        if 'salary_min' in mapping_adzuna and 'salary_max' in mapping_adzuna:
            min_col = mapping_adzuna['salary_min']
            max_col = mapping_adzuna['salary_max']

            if min_col in df_adzuna.columns and max_col in df_adzuna.columns:
                df_standardized_adzuna['salary'] = df_adzuna[min_col].apply(lambda x: f"${x}" if pd.notna(x) else "").astype(str) + \
                                                ' - ' + \
                                                df_adzuna[max_col].apply(lambda x: f"${x}" if pd.notna(x) else "").astype(str)
                df_standardized_adzuna['salary'] = df_standardized_adzuna['salary'].str.replace('$ - $', '', regex=False)
                df_standardized_adzuna['salary'] = df_standardized_adzuna['salary'].str.replace('$nan', '', regex=False)
                df_standardized_adzuna['salary'] = df_standardized_adzuna['salary'].str.replace('nan$', '', regex=False)
                df_standardized_adzuna['salary'] = df_standardized_adzuna['salary'].str.replace(' - ', '', regex=False)
            else:
                df_standardized_adzuna['salary'] = None
    
    if not df_jooble.empty:
        mapping_jooble = field_mappings['df_jooble']
        df_standardized_jooble['source'] = 'jooble'
        
        for new_col, original_col in mapping_jooble.items():
            if new_col != 'source':  
                if original_col in df_jooble.columns:
                    df_standardized_jooble[new_col] = df_jooble[original_col]
                else:
                    df_standardized_jooble[new_col] = None
    
    if not df_muse.empty:
        mapping_muse = field_mappings['df_muse']
        df_standardized_muse['source'] = 'muse'
        
        for new_col, original_col in mapping_muse.items():
            if new_col != 'source':  
                if '.' in original_col:
                    parts = original_col.split('.')
                    if parts[0] in df_muse.columns:
                        df_standardized_muse[new_col] = df_muse[parts[0]].apply(
                            lambda x: x.get(parts[1]) if isinstance(x, dict) and parts[1] in x else None
                        )
                    elif parts[0] == 'categories[0]' and 'categories' in df_muse.columns:
                        df_standardized_muse[new_col] = df_muse['categories'].apply(
                            lambda x: x[0].get('name') if isinstance(x, list) and len(x) > 0 and 'name' in x[0] else None
                        )
                    elif parts[0] == 'refs' and 'refs' in df_muse.columns:
                        df_standardized_muse[new_col] = df_muse['refs'].apply(
                            lambda x: x.get('landing_page') if isinstance(x, dict) and 'landing_page' in x else None
                        )
                else:
                    if original_col in df_muse.columns:
                        df_standardized_muse[new_col] = df_muse[original_col]
                    else:
                        df_standardized_muse[new_col] = None
    

    dfs_to_combine = []
    if not df_standardized_adzuna.empty:
        dfs_to_combine.append(df_standardized_adzuna)
    if not df_standardized_jooble.empty:
        dfs_to_combine.append(df_standardized_jooble)
    if not df_standardized_muse.empty:
        dfs_to_combine.append(df_standardized_muse)
    
    if not dfs_to_combine:
        logger.error("No data to combine after transformations")
        return None
    
    combined_df = pd.concat(dfs_to_combine, ignore_index=True)
    logger.info(f"Combined {len(combined_df)} total job records")
    
    destination_blob_name = f"{output_prefix}jobs_data_standardized.csv"
    upload_success = upload_to_gcs(combined_df, destination_blob_name, bucket_name)
    
    if upload_success:
        logger.info(f"Job data transformation complete. Result saved to {destination_blob_name}")
        return combined_df
    else:
        logger.error("Failed to upload transformed data")
        return None

def clean_jobs(event=None, context=None):
    try:
        result = transform_job_data()
        if result is not None:
            return "Job data cleaning and transformation completed successfully"
        else:
            return "Job data cleaning and transformation failed"
    except Exception as e:
        logger.error(f"Error in clean_jobs: {str(e)}")
        raise

if __name__ == "__main__":
    clean_jobs()