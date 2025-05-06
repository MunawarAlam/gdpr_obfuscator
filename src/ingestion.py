import csv
import json
import os
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import awswrangler as wr
from io import StringIO
import botocore

# Initialize the S3 client outside of the handler
s3_client = boto3.client('s3')

# Initialize the logger
logger = logging.getLogger()
logger.setLevel("INFO")

class GdprObfuscator:
    def __init__(self, ingestion_bucket=''):
        self.s3_client = boto3.client('s3')
        self.ingestion_bucket = ingestion_bucket
        self.obfuscated_bucket = 'ma-temp-processed-bucket'
        self.chunk_size = 1000
        self.buck_key = ''
        self.pii_fields = []
        self.s3_ingestion_path = ''

def replace_string(strg):
    return ("**********")


def set_initial_input(json_string):
    # Read JSON file
    dict_obj = json.loads(json_string)

    get_file_location = dict_obj['file_to_obfuscate'].split('/')

    initial_bucket = get_file_location[2]
    dir_name = get_file_location[3]
    file_name = get_file_location[4]

    initial_bucket = initial_bucket.replace("_","-")
    #
    gdpr_init.ingestion_bucket = initial_bucket
    gdpr_init.pii_fields = dict_obj['pii_fields']
    gdpr_init.buck_key = f'{dir_name}/{file_name}'
    gdpr_init.s3_ingestion_path = f's3://{gdpr_init.ingestion_bucket}/{dir_name}/{file_name}'

    return #dict_obj

def obfuscator_process(df, pii):
    new_pii = [pi.lower() for pi in pii]
    for df_c in df.columns:
        if df_c.lower() in new_pii:
            df[df_c] = df[df_c].apply(replace_string)
    return df




# def save_to_s3(data, bucket_name, filename, client):
#     '''
#     Saves the data passes under a specific key in a specified S3 bucket.
#
#     Args:
#         data: the data to be saved to s3
#         bucket_name: The S3 bucket name.
#         filename: the key to save the object under
#         client: The S3 client to interact with S3.
#
#     Returns:
#         str: None.'''
#     data_JSON = json.dumps(data)
#     client.put_object(
#         Bucket=bucket_name,
#         Body=data_JSON,
#         Key=filename
#     )

def object_exist_check():
    try:
        gdpr_init.s3_client.head_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
        delete_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key)
    except botocore.exceptions.ClientError as e:
        print("No Obfuscator Found, Creating New..")


def gdpr_csv():
    object_exist_check()
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
            create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer_d2.getvalue())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                #print(f"File: '{gdpr_init.buck_key}' does not exist!, creating new file...")
                create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer.getvalue())
            else:
                return ("Something else went wrong")
                raise
    return "Obfuscator process is completed successfully"

def create_s3_object(bucket, key, body):
    gdpr_init.s3_client.put_object(Bucket=bucket, Key=key, Body=body)

def delete_s3_object(bucket, key):
    gdpr_init.s3_client.delete_object(Bucket=bucket, Key=key)

def getting_access_to_file(initial_input):
    set_initial_input(initial_input)
    print(gdpr_init.pii_fields)
    try:
        gdpr_init.s3_client.head_bucket(Bucket=gdpr_init.ingestion_bucket)
        print("Obfuscator process Started..")
        msg = gdpr_csv()
        print(msg)
    except Exception:
        print("S3 Object does not exist")
        return

    # print(gdpr_init.s3_ingestion_path)
    # print(gdpr_init.buck_key)

    # for chunk in wr.s3.read_csv(path=gdpr_init.s3_ingestion_path, chunksize=gdpr_init.chunk_size):
    #     csv_buffer = StringIO()
    #     initial_df = pd.DataFrame(chunk)
    #     gdpr_df = obfuscator_process(initial_df, gdpr_init.pii_fields)
    #     gdpr_df.reset_index(drop=True, inplace=True)
    #     gdpr_df.to_csv(csv_buffer, index=False)
    #     #
    #     try:
    #         #Checking if object is exist
    #         gdpr_init.s3_client.head_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
    #         #
    #         csv_buffer_d2 = StringIO()
    #         s3_obj_req = gdpr_init.s3_client.get_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key)
    #         get_csv_data = pd.read_csv(s3_obj_req['Body'])
    #         get_csv_data.reset_index(drop=True, inplace=True)
    #         # Merge Data
    #         csv_merge_data = pd.concat([get_csv_data, gdpr_df], axis=0)
    #         csv_merge_data.to_csv(csv_buffer_d2, index=False)
    #         # Creating Process Object
    #
    #         create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer_d2.getvalue())
    #         # gdpr_init.s3_client.put_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key,
    #         #                                Body=csv_buffer_d2.getvalue())
    #         print("Work in progress.....")
    #     except botocore.exceptions.ClientError as e:
    #         if e.response["Error"]["Code"] == "404":
    #             print(f"File: '{gdpr_init.buck_key}' does not exist!, creating new file...")
    #             create_s3_object(gdpr_init.obfuscated_bucket, gdpr_init.buck_key, csv_buffer.getvalue())
    #             # gdpr_init.s3_client.put_object(Bucket=gdpr_init.obfuscated_bucket, Key=gdpr_init.buck_key,
    #             #                                Body=csv_buffer.getvalue())
    #         else:
    #             print("Something else went wrong")
    #             raise
    #         # return
    # print('--------')
    # print('Obfuscator process is completed..')

    #print(new_df)
    # ## write the data
    # data = read_file(bucket_name, key)
    # if data is None:
    #     print("No data due to key error.")
    # else:
    #     print("File reading is successful. Writing to a csv file.")
    #     data.to_csv('./output/data.csv', index=False)
    # ##
    ##
    #data = s3_obj_req['Body'].read().decode('utf-8').splitlines()
    #records = csv.reader(data)
    #header = next(records)
    ##

    #print(s3_object_req)

    # dir_name = get_file_location[3]
    # file_name = get_file_location[4]
    #print(initial_bucket, dir_name, file_name)


def upload_receipt_to_s3(bucket_name, key, receipt_content):
    """Helper function to upload receipt to S3"""

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=receipt_content
        )
    except Exception as e:
        logger.error(f"Failed to upload receipt to S3: {str(e)}")
        raise
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
        order_id = event['Order_id']
        amount = event['Amount']
        item = event['Item']

        # Access environment variables
        #bucket_name = os.environ.get('RECEIPT_BUCKET')
        bucket_name = "gdpr-ingestion-bucket"
        if not bucket_name:
            print(bucket_name)
            raise ValueError("Missing required environment variable RECEIPT_BUCKET")

        # Create the receipt content and key destination
        receipt_content = (
            f"OrderID: {order_id}\n"
            f"Amount: ${amount}\n"
            f"Item: {item}"
        )
        key = f"receipts/{order_id}.txt"

        # Upload the receipt to S3
        put_bucket_name = "ma-gdpr-processed-bucket"
        upload_receipt_to_s3(put_bucket_name, key, receipt_content)
        #upload_receipt_to_s3(bucket_name, key, receipt_content)

        logger.info(f"Successfully processed order {order_id} and stored receipt in S3 bucket {bucket_name}")

        return {
            "statusCode": 200,
            "message": "Receipt processed successfully"
        }

    except Exception as e:
        logger.error(f"Error processing order: {str(e)}")
        raise

if __name__ == "__main__":
    json_string = '{"file_to_obfuscate": "s3://ma-temp-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    gdpr_init = GdprObfuscator()
    getting_access_to_file(json_string)
# lambda_handler({
#     "Order_id": "12345",
#     "Amount": 199.99,
#     "Item": "Wireless Headphones"
# },'')