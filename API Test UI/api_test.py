"""
API Test UI
-----------
A simple Flask web application to test API connections for the Job Market Data Pipeline.

The muse API:
* Label: Inst767 Final Project	
* Key: 63f270eed3c05b2603cfaa96837b66c68986c8f6427bbbfd3dcedb9adafc9460

Adzuna API:
* ID: d7555b82
* Key: 32c5d58f0b8a9abb6474ec0568fcbe2d	
"""
from flask import Flask, render_template, request, jsonify, flash
import os
import json
import time
import requests
import traceback
import re
from datetime import datetime

# Import our connector modules
# Adjust the path if needed
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DAGs.muse_api import MuseConnector
from DAGs.adzuna_api import AdzunaConnector
from DAGs.hacker_news_api import HackerNewsConnector

app = Flask(__name__)
app.secret_key = os.urandom(24)

# In-memory storage for test results
test_results = {
    'muse': {'status': None, 'timestamp': None, 'data': None, 'error': None},
    'adzuna': {'status': None, 'timestamp': None, 'data': None, 'error': None},
    'hackernews': {'status': None, 'timestamp': None, 'data': None, 'error': None}
}

@app.route('/')
def index():
    """Render the main page with API test forms."""
    return render_template('index.html', test_results=test_results)

# @app.route('/test/muse', methods=['POST'])
# def test_muse():
#     """Test The Muse API connection with detailed diagnostics."""
#     api_key = request.form.get('api_key')
    
#     if not api_key:
#         flash('API Key is required', 'error')
#         return jsonify({'status': 'error', 'message': 'API Key is required'})
    
#     try:
#         base_url = "https://www.themuse.com/api/public/jobs"
        
#         # Try different parameter combinations
#         parameter_sets = [
#             # Try with no filters first
#             {"api_key": api_key, "page": 1, "page_size": 10},
#             # Try with a category
#             {"api_key": api_key, "category": "Software Engineering", "page": 1, "page_size": 10},
#             # Try with a different category
#             {"api_key": api_key, "category": "Marketing", "page": 1, "page_size": 10},
#             # Try with a location if available
#             {"api_key": api_key, "location": "New York", "page": 1, "page_size": 10}
#         ]
        
#         successful_params = None
#         jobs = []
        
#         start_time = time.time()
        
#         # Try each parameter set until we get results
#         for params in parameter_sets:
#             print(f"Trying with parameters: {params}")
#             response = requests.get(base_url, params=params)
            
#             if response.status_code == 200:
#                 data = response.json()
#                 current_jobs = data.get("results", [])
                
#                 print(f"Response with params {params}: {len(current_jobs)} jobs found")
                
#                 if current_jobs:
#                     jobs = current_jobs
#                     successful_params = params
#                     break
        
#         # If all parameter sets failed, use the last response
#         if not jobs:
#             print("All parameter sets returned 0 jobs. Using last response for diagnosis.")
#             response = requests.get(base_url, params=parameter_sets[-1])
#             data = response.json()
#             print(f"Full API response: {json.dumps(data, indent=2)}")
            
#             # Check if the response contains any data at all
#             if 'results' not in data:
#                 print(f"API response doesn't contain 'results' key. Available keys: {data.keys()}")
            
#             # Check if there's an error message
#             if 'error' in data:
#                 print(f"API returned an error: {data['error']}")
        
#         # Store results
#         test_results['muse'] = {
#             'status': 'success',
#             'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#             'data': jobs,
#             'error': None,
#             'count': len(jobs),
#             'time_taken': f"{time.time() - start_time:.2f} seconds",
#             'parameters_used': successful_params or parameter_sets[-1]
#         }
        
#         return jsonify({
#             'status': 'success', 
#             'count': len(jobs),
#             'time_taken': f"{time.time() - start_time:.2f} seconds",
#             'parameters_used': successful_params or parameter_sets[-1]
#         })
    
#     except Exception as e:
#         error_trace = traceback.format_exc()
#         print(f"Error in test_muse: {str(e)}")
#         print(error_trace)
#         test_results['muse'] = {
#             'status': 'error',
#             'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#             'data': None,
#             'error': str(e),
#             'error_trace': error_trace
#         }
#         return jsonify({'status': 'error', 'message': str(e), 'trace': error_trace})


@app.route('/test/adzuna', methods=['POST'])
def test_adzuna():
    
    app_id = request.form.get('app_id')
    app_key = request.form.get('app_key')
    keyword = request.form.get('keyword')
    
    if not app_id or not app_key:
        flash('App ID and App Key are required', 'error')
        return jsonify({'status': 'error', 'message': 'App ID and App Key are required'})
    
    try:
        keywords = str(keyword).split(',')
        print(keywords)
        connector = AdzunaConnector(app_id, app_key)
        jobs = connector.extract_jobs(keywords=keywords, max_pages=1)
        
        
        # Store results
        test_results['adzuna'] = {
            'status': 'success',
            'data': jobs,
            'error': None,
            'count': len(jobs)
        }
        
        return jsonify({
            'status': 'success', 
            'count': len(jobs)
        })
    
    except Exception as e:
        error_trace = traceback.format_exc()
        test_results['adzuna'] = {
            'status': 'error',
            'data': None,
            'error': str(e),
            'error_trace': error_trace
        }
        return jsonify({'status': 'error', 'message': str(e), 'trace': error_trace})

@app.route('/test/hackernews', methods=['POST'])
def test_hackernews():
    """Test HackerNews API connection without using Google Cloud."""
    try:
        start_time = time.time()
        
        # Directly find the hiring thread using Algolia API
        query = "Ask HN: Who is hiring?"
        algolia_url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": "points>20"  # Hiring threads usually have high point counts
        }
        
        response = requests.get(algolia_url, params=params)
        response.raise_for_status()
        results = response.json().get('hits', [])
        
        # Find the most relevant thread
        thread_id = None
        for result in results:
            if "hiring" in result.get('title', '').lower() and "ask hn" in result.get('title', '').lower():
                thread_id = result.get('objectID')
                break
        
        if not thread_id:
            raise Exception("Could not find a recent 'Who's hiring' thread")
        
        # Get comments from the thread using Hacker News API
        hn_api_url = f"https://hacker-news.firebaseio.com/v0/item/{thread_id}.json"
        response = requests.get(hn_api_url)
        response.raise_for_status()
        thread_data = response.json()
        
        # Get the first 5 comment IDs
        comment_ids = thread_data.get('kids', [])[:]
        
        # Fetch each comment
        comments = []
        for kid_id in comment_ids:
            comment_url = f"https://hacker-news.firebaseio.com/v0/item/{kid_id}.json"
            comment_response = requests.get(comment_url)
            comment_response.raise_for_status()
            comment_data = comment_response.json()
            
            # Only add non-deleted, non-dead comments
            if comment_data and not comment_data.get('deleted', False) and not comment_data.get('dead', False):
                comments.append(comment_data)
        
        # Parse the comments into job data
        jobs = []
        for comment in comments:
            if not comment or 'text' not in comment:
                continue
                
            # Create a basic job object from the comment
            job = {
                'job_id': f"hn-{comment.get('id')}",
                'posted_date': datetime.fromtimestamp(comment.get('time', 0)).strftime('%Y-%m-%d'),
                'author': comment.get('by', ''),
                'description': comment.get('text', ''),
                'source_api': 'hackernews',
                'source_url': f"https://news.ycombinator.com/item?id={comment.get('id')}"
            }
            
            # Extract basic company and title information with regex
            text = comment.get('text', '')
            
            # Try to find company name
            company_match = re.search(r'^([^|:]+)(?:\s*[|:]\s*|\s+is\s+hiring)', text, re.IGNORECASE | re.MULTILINE)
            if company_match:
                job['company'] = company_match.group(1).strip()
            
            # Try to find job title
            title_match = re.search(r'(?:hiring|for|hiring for|looking for)[^|:]*?([^|:,]*?(?:engineer|developer|designer|manager|director|lead|architect|consultant|scientist|specialist)[^|:,]*?)(?:at|\.|,|\||$)', text, re.IGNORECASE)
            if title_match:
                job['title'] = title_match.group(1).strip()
            
            # Try to find location
            if re.search(r'\bREMOTE\b', text, re.IGNORECASE):
                job['location'] = 'Remote'
            
            # Add job to results if we extracted useful information
            if 'company' in job or 'title' in job:
                jobs.append(job)
        
        # Store results
        test_results['hackernews'] = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': jobs,
            'error': None,
            'count': len(jobs),
            'time_taken': f"{time.time() - start_time:.2f} seconds",
            'thread_id': thread_id
        }
        
        return jsonify({
            'status': 'success', 
            'count': len(jobs),
            'time_taken': f"{time.time() - start_time:.2f} seconds",
            'thread_id': thread_id
        })
    
    except Exception as e:
        error_trace = traceback.format_exc()
        test_results['hackernews'] = {
            'status': 'error',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': None,
            'error': str(e),
            'error_trace': error_trace
        }
        return jsonify({'status': 'error', 'message': str(e), 'trace': error_trace})

@app.route('/view-data/<source>')
def view_data(source):
    """View the data fetched from an API source."""
    if source not in test_results or test_results[source]['data'] is None:
        flash(f'No data available for {source}', 'error')
        return render_template('view_data.html', source=source, data=None)
    
    return render_template('view_data.html', 
                          source=source, 
                          data=test_results[source]['data'],
                          count=len(test_results[source]['data']))

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create the HTML templates
    index_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Job Market API Test UI</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .api-card {
                margin-bottom: 20px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }
            .api-card .card-header {
                font-weight: bold;
            }
            .result-badge {
                float: right;
            }
        </style>
    </head>
    <body>
        <div class="container mt-4">
            <h1>Job Market API Test UI</h1>
            <p class="lead">Use this interface to test the connections to the job market APIs.</p>
            
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }}">{{ message }}</div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
            
            <div class="row">
                <div class="col-md-4">
                    <div class="card api-card">
                        <div class="card-header bg-primary text-white">
                            The Muse API
                            {% if test_results.muse.status == 'success' %}
                                <span class="badge bg-success result-badge">Success</span>
                            {% elif test_results.muse.status == 'error' %}
                                <span class="badge bg-danger result-badge">Error</span>
                            {% endif %}
                        </div>
                        <div class="card-body">
                            <form id="muse-form">
                                <div class="mb-3">
                                    <label for="muse-api-key" class="form-label">API Key</label>
                                    <input type="text" class="form-control" id="muse-api-key" name="api_key" required>
                                </div>
                                <button type="submit" class="btn btn-primary" id="muse-test-btn">Test Connection</button>
                                
                                {% if test_results.muse.status == 'success' %}
                                    <a href="/view-data/muse" class="btn btn-outline-primary mt-2">View Data</a>
                                    <div class="alert alert-success mt-3">
                                        <strong>Success!</strong><br>
                                        Fetched {{ test_results.muse.count }} jobs<br>
                                        Time: {{ test_results.muse.time_taken }}<br>
                                        Timestamp: {{ test_results.muse.timestamp }}
                                    </div>
                                {% elif test_results.muse.status == 'error' %}
                                    <div class="alert alert-danger mt-3">
                                        <strong>Error:</strong> {{ test_results.muse.error }}
                                    </div>
                                {% endif %}
                            </form>
                        </div>
                    </div>
                </div>
                
                <div class="col-md-4">
                    <div class="card api-card">
                        <div class="card-header bg-success text-white">
                            Adzuna API
                            {% if test_results.adzuna.status == 'success' %}
                                <span class="badge bg-success result-badge">Success</span>
                            {% elif test_results.adzuna.status == 'error' %}
                                <span class="badge bg-danger result-badge">Error</span>
                            {% endif %}
                        </div>
                        <div class="card-body">
                            <form id="adzuna-form">
                                <div class="mb-3">
                                    <label for="adzuna-app-id" class="form-label">App ID</label>
                                    <input type="text" class="form-control" id="adzuna-app-id" name="app_id" required>
                                </div>
                                <div class="mb-3">
                                    <label for="adzuna-app-key" class="form-label">App Key</label>
                                    <input type="text" class="form-control" id="adzuna-app-key" name="app_key" required>
                                </div>
                                <div class="mb-3">
                                    <label for="adzuna-keyword" class="form-label">Keyword</label>
                                    <input type="text" class="form-control" id="adzuna-keyword" name="keyword" required>
                                </div>
                                <button type="submit" class="btn btn-success" id="adzuna-test-btn">Test Connection</button>
                                
                                {% if test_results.adzuna.status == 'success' %}
                                    <a href="/view-data/adzuna" class="btn btn-outline-success mt-2">View Data</a>
                                    <div class="alert alert-success mt-3">
                                        <strong>Success!</strong><br>
                                        Fetched {{ test_results.adzuna.count }} jobs<br>
                                    </div>
                                {% elif test_results.adzuna.status == 'error' %}
                                    <div class="alert alert-danger mt-3">
                                        <strong>Error:</strong> {{ test_results.adzuna.error }}
                                    </div>
                                {% endif %}
                            </form>
                        </div>
                    </div>
                </div>
                
                <div class="col-md-4">
                    <div class="card api-card">
                        <div class="card-header bg-warning text-dark">
                            HackerNews API
                            {% if test_results.hackernews.status == 'success' %}
                                <span class="badge bg-success result-badge">Success</span>
                            {% elif test_results.hackernews.status == 'error' %}
                                <span class="badge bg-danger result-badge">Error</span>
                            {% endif %}
                        </div>
                        <div class="card-body">
                            <p>No credentials required for HackerNews API.</p>
                            <button type="button" class="btn btn-warning" id="hackernews-test-btn">Test Connection</button>
                            
                            {% if test_results.hackernews.status == 'success' %}
                                <a href="/view-data/hackernews" class="btn btn-outline-warning mt-2">View Data</a>
                                <div class="alert alert-success mt-3">
                                    <strong>Success!</strong><br>
                                    Fetched {{ test_results.hackernews.count }} jobs<br>
                                    Thread ID: {{ test_results.hackernews.thread_id }}<br>
                                    Time: {{ test_results.hackernews.time_taken }}<br>
                                    Timestamp: {{ test_results.hackernews.timestamp }}
                                </div>
                            {% elif test_results.hackernews.status == 'error' %}
                                <div class="alert alert-danger mt-3">
                                    <strong>Error:</strong> {{ test_results.hackernews.error }}
                                </div>
                            {% endif %}
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                // The Muse API Test
                document.getElementById('muse-form').addEventListener('submit', function(e) {
                    e.preventDefault();
                    const apiKey = document.getElementById('muse-api-key').value;
                    const btn = document.getElementById('muse-test-btn');
                    
                    btn.disabled = true;
                    btn.innerHTML = 'Testing...';
                    
                    fetch('/test/muse', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                        },
                        body: `api_key=${encodeURIComponent(apiKey)}`
                    })
                    .then(response => response.json())
                    .then(data => {
                        window.location.reload();
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        window.location.reload();
                    });
                });
                
                // Adzuna API Test
                document.getElementById('adzuna-form').addEventListener('submit', function(e) {
                    e.preventDefault();
                    const appId = document.getElementById('adzuna-app-id').value;
                    const appKey = document.getElementById('adzuna-app-key').value;
                    const btn = document.getElementById('adzuna-test-btn');
                    
                    btn.disabled = true;
                    btn.innerHTML = 'Testing...';
                    
                    fetch('/test/adzuna', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                        },
                        body: `app_id=${encodeURIComponent(appId)}&app_key=${encodeURIComponent(appKey)}`
                    })
                    .then(response => response.json())
                    .then(data => {
                        window.location.reload();
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        window.location.reload();
                    });
                });
                
                // HackerNews API Test
                document.getElementById('hackernews-test-btn').addEventListener('click', function() {
                    const btn = document.getElementById('hackernews-test-btn');
                    
                    btn.disabled = true;
                    btn.innerHTML = 'Testing...';
                    
                    fetch('/test/hackernews', {
                        method: 'POST'
                    })
                    .then(response => response.json())
                    .then(data => {
                        window.location.reload();
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        window.location.reload();
                    });
                });
            });
        </script>
    </body>
    </html>
    """
    
    view_data_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>{{ source|capitalize }} Data</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            pre {
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 5px;
                max-height: 500px;
                overflow-y: auto;
            }
        </style>
    </head>
    <body>
        <div class="container mt-4">
            <nav aria-label="breadcrumb">
                <ol class="breadcrumb">
                    <li class="breadcrumb-item"><a href="/">Home</a></li>
                    <li class="breadcrumb-item active">{{ source|capitalize }} Data</li>
                </ol>
            </nav>
            
            <div class="card">
                <div class="card-header">
                    <h3>{{ source|capitalize }} API Data <span class="badge bg-primary">{{ count }} items</span></h3>
                </div>
                <div class="card-body">
                    {% if data %}
                        <div class="mb-4">
                            <h4>Data Preview</h4>
                            {% for job in data[:] %}
                                <div class="card mb-3">
                                    <div class="card-body">
                                        {% if source == 'muse' %}
                                            <h5>{{ job.name }}</h5>
                                            <p><strong>Company:</strong> {{ job.company.name if job.company else 'N/A' }}</p>
                                            <p><strong>ID:</strong> {{ job.id }}</p>
                                            {% if job.locations %}
                                                <p><strong>Location:</strong> {{ job.locations[0].name if job.locations else 'N/A' }}</p>
                                            {% endif %}
                                            <p><strong>Publication Date:</strong> {{ job.publication_date }}</p>
                                        {% elif source == 'adzuna' %}
                                            <h5>{{ job.title }}</h5>
                                            <p><strong>Company:</strong> {{ job.company.display_name if job.company else 'N/A' }}</p>
                                            <p><strong>ID:</strong> {{ job.id }}</p>
                                            <p><strong>Location:</strong> {{ job.location.display_name if job.location else 'N/A' }}</p>
                                            <p><strong>Created:</strong> {{ job.created }}</p>
                                            {% if job.salary_min and job.salary_max %}
                                                <p><strong>Salary:</strong> ${{ job.salary_min }} - ${{ job.salary_max }}</p>
                                            {% endif %}
                                        {% elif source == 'hackernews' %}
                                            <h5>{{ job.title if job.title else 'Job at ' + job.company if job.company else 'Hacker News Job Post' }}</h5>
                                            <p><strong>Company:</strong> {{ job.company if job.company else 'N/A' }}</p>
                                            <p><strong>ID:</strong> {{ job.job_id }}</p>
                                            <p><strong>Posted Date:</strong> {{ job.posted_date }}</p>
                                            <p><strong>Location:</strong> {{ job.location if job.location else 'N/A' }}</p>
                                            {% if job.skills_required %}
                                                <p><strong>Skills:</strong> {{ ', '.join(job.skills_required) }}</p>
                                            {% endif %}
                                        {% endif %}
                                    </div>
                                </div>
                            {% endfor %}
                        </div>
                        
                        <h4>Raw JSON Data</h4>
                        <pre><code>{{ data|tojson(indent=2) }}</code></pre>
                    {% else %}
                        <div class="alert alert-warning">No data available.</div>
                    {% endif %}
                    
                    <a href="/" class="btn btn-primary mt-3">Back to Home</a>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Write the templates to files
    with open('templates/index.html', 'w') as f:
        f.write(index_html)
    
    with open('templates/view_data.html', 'w') as f:
        f.write(view_data_html)
    
    # Start the Flask server
    app.run(debug=True, port=8080)