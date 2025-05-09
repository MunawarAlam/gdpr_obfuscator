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
    def __init__(self, ingestion_bucket=''):
        self.s3_client = boto3.client('s3')
        self.ingestion_bucket = ingestion_bucket
        self.obfuscated_bucket = 'ma-gdpr-processed-bucket'
        self.chunk_size = 1000
        self.buck_key = ''
        self.pii_fields = []
        self.s3_ingestion_path = ''

def replace_string(strg):
    return ("**********")

def set_initial_input(input_string, gdpr_init):
    # Read JSON file
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
    return #dict_obj

def obfuscator_process(df, pii):
    new_pii = [pi.lower() for pi in pii]
    for df_c in df.columns:
        if df_c.lower() in new_pii:
            df[df_c] = df[df_c].apply(replace_string)
    return df

def object_exist_check(gdpr_init):
    try:
        gdpr_init.s3_client.head_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
        delete_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, gdpr_init)
    except botocore.exceptions.ClientError as e:
        logger.info("No Obfuscator file found from S3: bucket=%s, key=%s", gdpr_init.obfuscated_bucket, gdpr_init.buck_key)
        print(e, ": No Obfuscator File Found, new object file will create...")


def gdpr_csv(gdpr_init):
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
    gdpr_init.s3_client.put_object(Bucket=bucket, Key=key, Body=body)

def delete_s3_object(bucket, key, gdpr_init):
    logger.info("Existing file deleted")
    gdpr_init.s3_client.delete_object(Bucket=bucket, Key=key)

def getting_access_to_file(initial_input, gdpr_init):
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
    Main Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
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
    #input_string = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    #{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}
    input_string = sys.argv[1]
    gdpr_init = GdprObfuscator()
    getting_access_to_file(input_string, gdpr_init)
