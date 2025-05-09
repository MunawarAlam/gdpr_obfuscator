# GDPR Obfuscator Project Information

## Standard Requirements

The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and intercept personally identifiable information (PII). 

Data is stored in CSV-, JSON-, or parquet-formatted files in an AWS S3 bucket.
Fields containing GDPR-sensitive data are known and will be supplied in advance.
Data records will be supplied with a primary key.

The tool will be supplied with the S3 location of a file containing sensitive information, and the names of the affected fields. It will create a new file or byte-stream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings. The calling procedure will handle saving the output to its destination. It is expected that the tool will be deployed within the AWS account.

## The Product 
The application library is designed to extract data from a specified source and mask predefined Personally Identifiable Information (PII) fields by replacing their values with *****.

It can be invoked manually via AWS Lambda or automated using AWS EventBridge or Step Functions for scheduled execution.

The library also supports local execution through the AWS CLI, enabling testing and development outside of the cloud environment.

## Technical Details 
1. The application is provisioned using Terraform, which sets up the initial infrastructure components including the S3 buckets, AWS Lambda function, EventBridge rule, and Step Function workflow.

2. AWS EventBridge is configured to allow scheduling based on individual requirements, enabling flexible automated execution.

3. An AWS Lambda function is deployed, which can be invoked with a payload specifying the S3 bucket, file name, and list of PII fields to obfuscate.

4. Input validation is implemented to ensure that the application only processes valid files and correctly defined PII fields.

5. The Obfuscator processes the ingested file and writes the obfuscated output to a new location in a designated S3 bucket. 

6. A separate “processed” S3 bucket is used to store obfuscated files, ensuring that downstream users can access sanitized data without exposing sensitive information.


## Sample Data
Two sample student datasets were used to evaluate application performance.

The first sample was under 1 MB, while the second (with identical data) exceeded 1 MB. Both datasets were processed successfully in under one minute.

## Possible Extensions
The application architecture is designed to be extensible. Support for additional file formats such as JSON and Parquet can be integrated by implementing new processing functions and incorporating them into the existing control flow via conditional logic (e.g., a format switch handler).

## Requirements
Python Version: 3.12.1

# Instructions
1. Configure GitHub Secrets
Add the following secrets to your GitHub repository:

    - SAFETY_API_KEY – for safety and vulnerability checks
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_REGION


2.  <b>Push to GitHub:</b>

    A push to the configured branch will trigger the CI/CD pipeline to publish the code and create the necessary S3 buckets


3. <b> Upload Data:</b>

    Upload the obfuscated .csv file to the appropriate S3 bucket as configured in your deployment. 


4. <b>Branch Configuration</b>

    Ensure your working branch is listed under the on > push > branches section of the GitHub Actions YAML file.


5. <b>Deploy:</b>

    Push your changes to GitHub. If all tests pass and the required conditions are met, the code will be automatically deployed to your AWS account.
