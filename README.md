# AWS Basic Lambda Project
## Overview
This project includes Lambda functions and configurations for processing AWS services. 

Originally, the file was a .json file, so I used pandas to convert that to .csv to streamline the Lambda process.

Lambda function listens for PUT or GET object and executes and places updated object in secondary folder within the bucket.

## Requirements
- AWS CLI
- Python 3.x

## Setup
1. Bucket creation
2. Update bucket permissions
3. Create Lambda function as Python - 
4. Update event notifications for bucket 