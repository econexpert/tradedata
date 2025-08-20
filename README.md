# tradedata

Fetches **UN Comtrade trade data** into **AWS S3** using an **AWS Lambda** function.  
Resumable, fault-tolerant, and can be triggered via **Function URL** or **AWS SDK**.  

---
## Features

Runs as an AWS Lambda (invoked via Function URL or API Gateway).

Fetches trade data from the Comtrade API
.

Saves results into S3 JSON arrays.

Keeps a progress file in S3 to resume after timeouts.

Retries on 403 errors with backoff.

Automatically stops before the 15-minute Lambda time limit.

## Requirements

AWS Lambda (Python 3.11 runtime recommended).

An S3 bucket for storing results + progress + country codes.

A Comtrade API key.

---

## Setup
### 1. Create an S3 bucket

Create an S3 bucket to hold:

Country codes file (country-codes.json).

Progress files (progress/...).

Results files (results/...).

### 2. Upload country codes

Upload your country-codes.json into the S3 bucket.
The Lambda will read this to map numeric country IDs to names.

### 3. Create the Lambda function

Runtime: Python 3.11

Upload the code from this repo (lambda_function.py).

Increase timeout to 15 minutes.

Increase memory as needed (512 MB or more recommended).

### 4. Set environment variables

In your Lambda configuration, add:

COMTRADE_API_KEY → your Comtrade API key
.

COUNTRY_CODES_KEY → the key (path) of the country-codes.json file in your S3 bucket.

S3_BUCKET_NAME → your S3 bucket name.

### 5. Add IAM permissions

Attach a policy to your Lambda’s IAM role allowing read/write access to the S3 bucket, e.g.:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject"
  ],
  "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
}
```

---

## Event JSON:

{
  "region_of_interest": "428",
  "trade_regime": "2",
  "start_year": "1995",
  "end_year": "2024"
}

Environment variables: 
COMTRADE_API_KEY
COUNTRY_CODES_KEY
S3_BUCKET_NAME

---


## Architecture

```mermaid
flowchart TD
    A[User / Client] -->|Invoke Function URL with params| B[Lambda Function]
    B -->|Fetch trade data| C[Comtrade API]
    B -->|Save results.json| D[S3 Bucket]
    B -->|Save progress.json| D



