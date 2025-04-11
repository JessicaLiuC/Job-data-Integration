# Job Market API Test UI

A simple web-based testing interface for validating the API connections in the Job Market Data Integration Pipeline.

## Overview

This Flask application provides a user-friendly web interface to test connections to all three job data APIs:
- The Muse API
- Adzuna API
- Hacker News API

With this tool, you can easily validate that your API credentials are working and view sample data without writing any code.

## Features

- Test all three API connectors through a simple UI
- View detailed results and error messages
- Browse the retrieved job data in a structured format
- No need for command-line testing or writing test scripts

## Setup

1. Make sure you have Python 3.8+ installed
2. Install the required packages:

```bash
pip install flask requests google-cloud-storage
```

## Running the Test UI

1. Navigate to the project directory
2. Run the Flask application:

```bash
python api_test_ui.py
```

3. Open your browser and go to `http://localhost:8080`

## How to Use

### Testing The Muse API

1. Obtain an API key from [The Muse Developer Portal](https://www.themuse.com/developers/api/v2)
2. Enter your API key in the form
3. Click "Test Connection"
4. If successful, you'll see a green badge and can view the retrieved data

### Testing Adzuna API

1. Obtain App ID and App Key from [Adzuna API Dashboard](https://developer.adzuna.com/)
2. Enter your credentials in the form
3. Click "Test Connection"
4. If successful, you'll see a green badge and can view the retrieved data

### Testing Hacker News API

1. No credentials are required for the Hacker News API
2. Simply click "Test Connection"
3. If successful, you'll see a green badge and can view the retrieved data

## Viewing Data

After a successful test, click the "View Data" button to see:
- A preview of the first 5 job postings
- The complete raw JSON data

## Troubleshooting

If a test fails, you'll see:
- A red error badge
- The specific error message
- Detailed error information to help diagnose the problem

## Note

This is a development tool and is not intended for production use. It's designed to help you validate your API connections and view sample data during the development of your ETL pipeline.