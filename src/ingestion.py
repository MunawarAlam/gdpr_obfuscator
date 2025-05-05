import csv
import json
import os
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Initialize the S3 client outside of the handler
s3_client = boto3.client('s3')

# Initialize the logger
logger = logging.getLogger()
logger.setLevel("INFO")

class GdprObfuscator:
    def __init__(self, ingestion_bucket=''):
        self.s3_client = boto3.client('s3')
        self.ingestion_bucket = ingestion_bucket

def replace_string(strg):
    return ("**********")


def read_json_string(json_string):
    # Read JSON file
    dict_obj = json.loads(json_string)
    return dict_obj

def gdpr_process(df, pii):

    #f_name_col = df["First_Name"]
    #new_df = f_name_col.apply(replace_string)
    new_pii = [pi.lower() for pi in pii]

    for df_c in df.columns:
        if df_c.lower() in new_pii:
            print("field found.. ", df_c)
            df[df_c] = df[df_c].apply(replace_string)

    # initial_df["First_Name"] = new_df
    print(df.head(3))
    return df

def getting_access_to_file(initial_input):
    convert_json_dict = read_json_string(initial_input)
    get_file_location = convert_json_dict['file_to_obfuscate'].split('/')
    initial_bucket = get_file_location[2]
    initial_bucket = initial_bucket.replace("_","-")
    gdpr_init.ingestion_bucket = initial_bucket
    pii_fields = convert_json_dict['pii_fields']
    print(pii_fields)

    try:
        chk_bucket_exist = gdpr_init.s3_client.head_bucket(Bucket=gdpr_init.ingestion_bucket)
        #print(chk_bucket_exist)
    except Exception:
        print("not exist")
        return

    # all_objects = s3_client.list_objects_v2(Bucket=gdpr_init.ingestion_bucket, Prefix='new_data/')
    # all_key_timestamps = [item['Key'] for item in all_objects['Contents']]
    # print(all_key_timestamps)

    #print(chk_bucket_exist)

    #print(gdpr_init.ingestion_bucket)
    dir_name = get_file_location[3]
    file_name = get_file_location[4]
    buck_key = f'{dir_name}/{file_name}'
    print(buck_key)

    try:
        s3_obj_req = gdpr_init.s3_client.get_object(Bucket=gdpr_init.ingestion_bucket, Key=buck_key)
        initial_df = pd.read_csv(s3_obj_req['Body'])
        gdpr_data = gdpr_process(initial_df, pii_fields)

        #new_df = initial_df.loc[initial_df["First_Name"]] = "**"
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("Key doesn't match. Please check the key value entered.")
            return

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