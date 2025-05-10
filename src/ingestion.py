import csv
import json
import os
import logging
import sys
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import awswrangler as wr
from io import StringIO
import botocore
import re

# Initialize the logger
logger = logging.getLogger()
logger.setLevel("INFO")

class GdprObfuscator:


    """
    A class to handle GDPR obfuscation tasks on data stored in AWS S3 buckets.

    This class initializes the necessary S3 client and holds configuration
    for ingesting and processing data, including the source and destination
    buckets, chunk size, and fields identified as personally identifiable information (PII).
    """

    def __init__(self, ingestion_bucket=''):
        self.s3_client = boto3.client('s3')
        self.ingestion_bucket = ingestion_bucket
        self.obfuscated_bucket = 'ma-gdpr-processed-bucket'
        self.chunk_size = 8000
        self.buck_key = ''
        self.pii_fields = []
        self.s3_ingestion_path = ''

def replace_string(strg):
    """
        Replaces the input string with a fixed obfuscated value.

        Parameters:
            strg (str): The original string to be obfuscated.

        Returns:
            str: A fixed string of asterisks used to mask the original input.
        """
    return ("**********")

def set_initial_input(input_string, gdpr_init):
    """
    Parses an input CSV string or dictionary to extract S3 file location and PII field information,
    and updates the attributes of a GdprObfuscator instance accordingly.

    Parameters:
        input_string (str or dict): A JSON string or dictionary containing:
            - 'file_to_obfuscate': Full S3 path to the input file.
            - 'pii_fields': List of fields to obfuscate.
        gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class to update with parsed information.

    Returns:
        bool or None: Returns False if input is invalid or no PII fields are found. Returns None on successful setup.
    """
    try:
        dict_obj = json.loads(input_string)
    except TypeError as e:
        if isinstance(input_string, dict):
            dict_obj = input_string
        else:
            logger.info("Input type not supported")
            print(e, " - input type not supported")
            return
    except Exception as e:
        logger.info("Kunown error")
        print(e, "Kunown error")

    get_file_location = dict_obj['file_to_obfuscate'].split('/')
    initial_bucket = get_file_location[2]

    chk_file = re.findall(r"^[a-zA-Z0-9_-]*$", initial_bucket)
    #print(chk_file)
    if len(chk_file) == 0:
        return False

    dir_name = get_file_location[3]
    file_name = get_file_location[4]

    initial_bucket = initial_bucket.replace("_","-")
    #
    gdpr_init.ingestion_bucket = initial_bucket
    gdpr_init.pii_fields = dict_obj['pii_fields']

    if len(gdpr_init.pii_fields) == 0:
        print("No obfuscator field to process")
        return False
    gdpr_init.buck_key = f'{dir_name}/{file_name}'
    gdpr_init.s3_ingestion_path = f's3://{gdpr_init.ingestion_bucket}/{dir_name}/{file_name}'
    return

def obfuscator_process(df, pii):
    """
    Obfuscates specified PII fields in a pandas DataFrame by replacing their values with asterisks.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the data to process.
        pii (list of str): List of column names considered to contain personally identifiable information (PII).

    Returns:
        pandas.DataFrame: A DataFrame with PII fields obfuscated.
    """
    new_pii = [pi.lower() for pi in pii]
    for df_c in df.columns:
        if df_c.lower() in new_pii:
            df[df_c] = df[df_c].apply(replace_string)
    return df

def object_exist_check(gdpr_init):
    """
    Checks if the obfuscated file already exists in the destination S3 bucket.
    If it exists, deletes the existing object to allow overwriting.

    Parameters:
        gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class, containing S3 client,
                                    bucket names, and object key information.

    Side Effects:
        May delete an existing object in the obfuscated S3 bucket if found.
        Logs info messages and prints errors to the console.
    """

    try:
        gdpr_init.s3_client.head_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
        delete_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, gdpr_init)
    except botocore.exceptions.ClientError as e:
        logger.info("No Obfuscator file found from S3: bucket=%s, key=%s", gdpr_init.obfuscated_bucket, gdpr_init.buck_key)
        print(e, ": No Obfuscator File Found, new object file will create...")


def gdpr_csv(gdpr_init):
    """
    Processes a CSV file stored in S3 by reading it in chunks, obfuscating specified PII fields,
    and saving the obfuscated data back to an S3 bucket.

    If an obfuscated file already exists at the destination, the new obfuscated data is appended.
    Otherwise, a new file is created.

    Parameters:
        gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class containing S3
                                    configuration, chunk size, PII fields, and paths.

    Returns:
        str: A message indicating the completion of the obfuscation process.

    Side Effects:
        - Reads from and writes to S3.
        - Logs status updates.
        - May create or update files in the obfuscated S3 bucket.
    """

    object_exist_check(gdpr_init)
    for chunk in wr.s3.read_csv(path=gdpr_init.s3_ingestion_path, chunksize=gdpr_init.chunk_size):
        csv_buffer = StringIO()
        initial_df = pd.DataFrame(chunk)
        gdpr_df = obfuscator_process(initial_df, gdpr_init.pii_fields)
        gdpr_df.reset_index(drop=True, inplace=True)
        gdpr_df.to_csv(csv_buffer, index=False)
        #
        try:
            #Checking if object is exist
            gdpr_init.s3_client.head_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
            #
            csv_buffer_d2 = StringIO()
            s3_obj_req = gdpr_init.s3_client.get_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
            get_csv_data = pd.read_csv(s3_obj_req['Body'])
            get_csv_data.reset_index(drop=True, inplace=True)
            # Merge Data
            csv_merge_data = pd.concat([get_csv_data, gdpr_df], axis=0)
            csv_merge_data.to_csv(csv_buffer_d2, index=False)
            # Creating Process Object
            create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer_d2.getvalue(), gdpr_init)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                #print(f"File: '{gdpr_init.buck_key}' does not exist!, creating new file...")
                create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer.getvalue(), gdpr_init)
                logger.info("New Obfuscator file created in S3: bucket=%s, key=%s", gdpr_init.obfuscated_bucket,
                            gdpr_init.buck_key)
            else:
                return (e, "Something else went wrong")
                raise
    logger.info("Obfuscator file created in S3: bucket=%s, key=%s", gdpr_init.obfuscated_bucket,
           gdpr_init.buck_key)
    return "Obfuscator process is completed successfully"

def create_s3_object(bucket, key, body, gdpr_init):
    """
        Uploads an object to an S3 bucket using the provided parameters.

        Parameters:
            bucket (str): The name of the S3 bucket to upload the object to.
            key (str): The object key (i.e., path/filename) in the S3 bucket.
            body (str): The content to be uploaded (typically CSV or JSON data as a string).
            gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class containing the initialized S3 client.

        Side Effects:
            - Uploads the specified data to S3.
        """
    gdpr_init.s3_client.put_object(Bucket=bucket, Key=key, Body=body)

def delete_s3_object(bucket, key, gdpr_init):
    """
    Deletes an object from an S3 bucket.

    Parameters:
        bucket (str): The name of the S3 bucket containing the object.
        key (str): The key (i.e., path/filename) of the object to delete.
        gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class containing the initialized S3 client.

    Side Effects:
        - Deletes the specified object from S3.
        - Logs a message indicating the file was deleted.
    """

    logger.info("Existing file deleted")
    gdpr_init.s3_client.delete_object(Bucket=bucket, Key=key)

def getting_access_to_file(initial_input, gdpr_init):
    """
    Initiates the GDPR obfuscation process by setting up the input parameters and
    accessing the S3 file for processing.

    This function calls `set_initial_input` to parse the input and update the `gdpr_init`
    object, checks the existence of the source S3 bucket, and then begins the obfuscation
    process by calling `gdpr_csv`.

    Parameters:
        initial_input (str or dict): The initial input string or dictionary containing the S3 file
                                     path and PII field information.
        gdpr_init (GdprObfuscator): An instance of the GdprObfuscator class that holds configuration
                                    for the obfuscation process.

    Side Effects:
        - Initializes the `gdpr_init` object with relevant file paths and PII fields.
        - Logs information about the start of the obfuscation process.
        - Calls the `gdpr_csv` function to process the data.
        - Prints messages to the console and logs progress or errors.

    Returns:
        None: This function doesn’t return anything, but prints messages and logs actions.
    """

    set_initial_input(initial_input, gdpr_init)
    # print(gdpr_init.pii_fields)
    try:
        gdpr_init.s3_client.head_bucket(Bucket=gdpr_init.ingestion_bucket)
        logger.info("Obfuscator process Started..")
        print("Obfuscator process Started..")
        msg = gdpr_csv(gdpr_init)
        print(msg)
    except Exception as e:
        logger.info("S3 Object does not exist or..")
        print(e, "S3 Object does not exist or..")
        return

def lambda_handler(event, context):
    """
    AWS Lambda handler function to trigger the GDPR obfuscation process.

    This function is invoked by an event (typically an S3 trigger or API call). It parses the input
    event, initializes the `GdprObfuscator` class, and starts the process of obfuscating specified
    PII fields in the given file stored in an S3 bucket.

    Parameters:
        event (dict): The input event, typically containing the path to the S3 file and PII fields.
        context (LambdaContext): The runtime information of the Lambda function (unused in this case).

    Returns:
        dict: A response indicating the status of the obfuscation process.
            - statusCode: HTTP status code (200 for success).
            - message: A message indicating success or failure.

    Raises:
        Exception: If an error occurs during the obfuscation process, it logs the error and raises it.
    """

    try:
        # Parse the input event
        input_string = event
        #input_string = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
        #"s3://my_ingestion_bucket/new_data/file1.csv"
        gdpr_init = GdprObfuscator()
        getting_access_to_file(input_string, gdpr_init)
        logger.info(f"Successfully processed obfuscator")

        return {
            "statusCode": 200,
            "message": "Obfuscator processed successfully"
        }

    except Exception as e:
        logger.error(f"Error processing obfuscator: {str(e)}")
        raise

if __name__ == "__main__":
    """
    This block runs when the script is executed as the main module.
    It takes a command-line argument (input_string) and passes it to the obfuscation process.
    """

    #input_string = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    #{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}
    input_string = sys.argv[1]
    gdpr_init = GdprObfuscator()
    getting_access_to_file(input_string, gdpr_init)


