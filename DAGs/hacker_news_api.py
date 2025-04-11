"""
Hacker News API Connector
------------------------
Extracts job data from Hacker News' monthly "Who's Hiring" threads.
"""
import os
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import requests
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HackerNewsConnector:
    """Connector for extracting job data from HN 'Who's Hiring' posts."""
    
    HN_API_BASE = "https://hacker-news.firebaseio.com/v0"
    HN_ALGOLIA_API = "https://hn.algolia.com/api/v1"
    
    def __init__(self, gcs_bucket: str, max_retries: int = 3, retry_delay: int = 2):
        """
        Initialize Hacker News API connector.
        
        Args:
            gcs_bucket: Google Cloud Storage bucket name for storing raw data
            max_retries: Maximum number of retry attempts for API calls
            retry_delay: Delay between retries in seconds
        """
        self.gcs_bucket = gcs_bucket
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.storage_client = storage.Client()
    
    def extract_jobs(self, months_back: int = 1) -> List[Dict[str, Any]]:
        """
        Extract jobs from Hacker News 'Who's Hiring' thread.
        
        Args:
            months_back: How many months back to look for hiring threads
            
        Returns:
            List of job dictionaries parsed from comments
        """
        # Find the most recent "Who's hiring" thread
        thread_id = self._find_hiring_thread(months_back)
        if not thread_id:
            logger.error("Could not find a recent 'Who's hiring' thread")
            return []
        
        # Get the comments (job postings) from the thread
        job_comments = self._get_thread_comments(thread_id)
        logger.info(f"Extracted {len(job_comments)} job postings from Hacker News thread {thread_id}")
        
        # Parse the comments into structured job data
        job_data = [self._parse_job_comment(comment) for comment in job_comments]
        
        # Filter out None values (comments that couldn't be parsed)
        job_data = [job for job in job_data if job]
        logger.info(f"Successfully parsed {len(job_data)} jobs from Hacker News")
        
        return job_data
    
    def _find_hiring_thread(self, months_back: int = 1) -> Optional[int]:
        """
        Find the most recent 'Who's hiring' thread on Hacker News.
        
        Args:
            months_back: How many months back to search
            
        Returns:
            Thread ID of the hiring thread, or None if not found
        """
        # Get the approximate date of the thread we're looking for
        today = datetime.now()
        target_date = today.replace(day=1) - timedelta(days=1) if months_back == 1 else today.replace(day=1)
        target_month = (target_date.month - months_back) % 12 + 1
        target_year = target_date.year - ((target_date.month - target_month) // 12)
        
        # Format the search query
        month_name = datetime(target_year, target_month, 1).strftime('%B')
        query = f"Ask HN: Who is hiring? {month_name} {target_year}"
        
        try:
            # Search for the thread using Algolia API
            url = f"{self.HN_ALGOLIA_API}/search_by_date"
            params = {
                "query": query,
                "tags": "story",
                "numericFilters": "points>20"  # Hiring threads usually have high point counts
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get('hits', [])
            
            # Find the most relevant thread
            for result in results:
                if "hiring" in result.get('title', '').lower() and "ask hn" in result.get('title', '').lower():
                    return result.get('objectID')
            
            # If we didn't find an exact match, try a less specific search
            query = f"Who is hiring? {month_name}"
            params['query'] = query
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get('hits', [])
            
            for result in results:
                if "hiring" in result.get('title', '').lower():
                    return result.get('objectID')
                    
            return None
        except Exception as e:
            logger.error(f"Error finding hiring thread: {str(e)}")
            return None
    
    def _get_thread_comments(self, thread_id: int) -> List[Dict[str, Any]]:
        """
        Get all comments from a HN thread.
        
        Args:
            thread_id: Thread ID to fetch comments from
            
        Returns:
            List of comment dictionaries
        """
        comments = []
        
        try:
            # Get the thread item first
            url = f"{self.HN_API_BASE}/item/{thread_id}.json"
            response = requests.get(url)
            response.raise_for_status()
            thread_data = response.json()
            
            # Get all the kids (comments)
            if 'kids' in thread_data:
                for kid_id in thread_data['kids']:
                    for attempt in range(self.max_retries):
                        try:
                            comment_url = f"{self.HN_API_BASE}/item/{kid_id}.json"
                            comment_response = requests.get(comment_url)
                            comment_response.raise_for_status()
                            comment_data = comment_response.json()
                            
                            # Only add non-deleted, non-dead comments
                            if comment_data and not comment_data.get('deleted', False) and not comment_data.get('dead', False):
                                comments.append(comment_data)
                            
                            # Don't overwhelm the API
                            time.sleep(0.1)
                            break
                        except requests.RequestException:
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                logger.warning(f"Failed to fetch comment {kid_id} after {self.max_retries} attempts")
            
            return comments
        except Exception as e:
            logger.error(f"Error fetching comments for thread {thread_id}: {str(e)}")
            return []
    
    def _parse_job_comment(self, comment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a HN comment into structured job data.
        
        Args:
            comment: Comment dictionary from HN API
            
        Returns:
            Dictionary with parsed job data, or None if parsing failed
        """
        if not comment or 'text' not in comment:
            return None
        
        text = comment.get('text', '')
        
        # Basic job information
        job = {
            'job_id': f"hn-{comment.get('id')}",
            'posted_date': datetime.fromtimestamp(comment.get('time', 0)).strftime('%Y-%m-%d'),
            'author': comment.get('by', ''),
            'description': text,
            'source_api': 'hackernews',
            'source_url': f"https://news.ycombinator.com/item?id={comment.get('id')}",
            'raw_content': text
        }
        
        # Extract structured information using regex patterns
        job.update(self._extract_job_details(text))
        
        return job
    
    def _extract_job_details(self, text: str) -> Dict[str, Any]:
        """
        Extract structured job details from comment text using regex.
        
        Args:
            text: Comment text
            
        Returns:
            Dictionary with extracted job details
        """
        details = {}
        
        # Extract company name (often at the start or in the format "Company | Position")
        company_match = re.search(r'^([^|:]+)(?:\s*[|:]\s*|\s+is\s+hiring)', text, re.IGNORECASE | re.MULTILINE)
        if company_match:
            details['company'] = company_match.group(1).strip()
        
        # Extract job title
        title_patterns = [
            r'(?:hiring|for|hiring for|looking for)[^|:]*?([^|:,]*?(?:engineer|developer|designer|manager|director|lead|architect|consultant|scientist|specialist)[^|:,]*?)(?:at|\.|,|\||$)',
            r'(?:\||:)\s*([^|:,]*?(?:engineer|developer|designer|manager|director|lead|architect|consultant|scientist|specialist)[^|:,]*?)(?:at|\.|,|\||$)'
        ]
        
        for pattern in title_patterns:
            title_match = re.search(pattern, text, re.IGNORECASE)
            if title_match:
                details['title'] = title_match.group(1).strip()
                break
        
        # Extract location
        location_patterns = [
            r'(?:REMOTE|ONSITE|HYBRID|RELOCATION)(?:\s*(?:\/|,|\|)\s*(?:REMOTE|ONSITE|HYBRID|RELOCATION))*',
            r'(?:location|based in|located in|in)\s*:\s*([^\.]{3,50}?)(?:\.|,|\||$)',
            r'(?:\s|^)([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2,})(?:\s|$|\.|\|)',  # City, State format
        ]
        
        for pattern in location_patterns:
            location_match = re.search(pattern, text, re.IGNORECASE)
            if location_match:
                details['location'] = location_match.group(0).strip() if pattern.startswith('(?:REMOTE') else location_match.group(1).strip()
                break
        
        # Determine if remote
        if re.search(r'\bREMOTE\b', text, re.IGNORECASE):
            if 'location' not in details:
                details['location'] = 'Remote'
            details['remote'] = True
        
        # Extract employment type
        if re.search(r'\b(?:INTERN|INTERNSHIP)\b', text, re.IGNORECASE):
            details['employment_type'] = 'internship'
        elif re.search(r'\b(?:CONTRACT|CONTRACTOR|FREELANCE)\b', text, re.IGNORECASE):
            details['employment_type'] = 'contract'
        elif re.search(r'\b(?:PART[- ]TIME)\b', text, re.IGNORECASE):
            details['employment_type'] = 'part-time'
        else:
            details['employment_type'] = 'full-time'  # Default assumption
        
        # Extract skills (look for common programming languages and technologies)
        skills_pattern = r'\b(Python|JavaScript|JS|TypeScript|TS|Java|C\+\+|C#|Ruby|Go|Golang|Rust|PHP|Swift|Kotlin|SQL|React|Angular|Vue|Node\.js|Django|Flask|Spring|TensorFlow|PyTorch|AWS|GCP|Azure|Docker|Kubernetes|K8s|Terraform)\b'
        skills_matches = re.findall(skills_pattern, text, re.IGNORECASE)
        if skills_matches:
            details['skills_required'] = list(set(skill for skill in skills_matches))
        
        # Extract salary information if present
        salary_pattern = r'(?:salary|compensation|pay)(?:\s+is|\s*:\s*)?\s+\$?((?:\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)(?:\s*-\s*\$?(?:\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?))?(?:\s*[kK]\b)?)'
        salary_match = re.search(salary_pattern, text, re.IGNORECASE)
        if salary_match:
            details['salary_info'] = salary_match.group(1).strip()
        
        return details
    
    def save_to_gcs(self, jobs: List[Dict[str, Any]]) -> str:
        """
        Save extracted jobs to Google Cloud Storage.
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            GCS file path
        """
        if not jobs:
            logger.warning("No jobs to save to GCS")
            return ""
        
        date_str = datetime.now().strftime('%Y-%m-%d')
        file_path = f"raw/hackernews/jobs_{date_str}.json"
        
        bucket = self.storage_client.bucket(self.gcs_bucket)
        blob = bucket.blob(file_path)
        
        # Save as newline-delimited JSON for easier processing
        jobs_json = "\n".join(json.dumps(job) for job in jobs)
        blob.upload_from_string(jobs_json, content_type="application/json")
        
        logger.info(f"Saved {len(jobs)} jobs to gs://{self.gcs_bucket}/{file_path}")
        return f"gs://{self.gcs_bucket}/{file_path}"

# Example usage
if __name__ == "__main__":
    # For local testing
    gcs_bucket = os.environ.get("GCS_BUCKET")
    
    if gcs_bucket:
        connector = HackerNewsConnector(gcs_bucket)
        jobs = connector.extract_jobs()
        connector.save_to_gcs(jobs)
    else:
        logger.error("Missing required environment variable: GCS_BUCKET")