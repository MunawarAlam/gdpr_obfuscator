from src.ingestion import *
from unittest.mock import patch , Mock
import pytest
from moto import mock_aws
import boto3
from pprint import pprint
import json
#export PYTHONPATH=$(pwd)

class MockConnection:
    def run(self, query):
        return [
            ('S1000', "Omar", "Williams", "student1@university.com", "Female", 22),
            ('S1001', "Maria", "Brown", "student2@university.com", "Male", 18)
        ]
    @property
    def columns(self):
        return [
            {"name": "Student_ID"},
            {"name": "First_Name"},
            {"name": "Last_Name"},
            {"name": "Email"},
            {"name": "Gender"},
            {"name": "Age"}
        ]
gdpr_init = GdprObfuscator()

def test_bucket_name_converted_for_set_initial_input():
    input = '{"file_to_obfuscate": "s3://ma_gdpr_@ingestion_bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    output = set_initial_input(input, gdpr_init)
    print(output)
    assert output == False


def test_bucket_name_value_not_correct_for_set_initial_input():
    input = '{"file_to_obfuscate": "s3://ma_gdpr_ingestion_bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    set_initial_input(input, gdpr_init)
    output_bucket = "ma-gdpr-ingestion-bucket"
    assert output_bucket == gdpr_init.ingestion_bucket

def test_correct_input_file_been_sent_for_set_initial_input():
    input = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    set_initial_input(input, gdpr_init)
    output_bucket = "ma-gdpr-ingestion-bucket"
    assert output_bucket == gdpr_init.ingestion_bucket


def test_gdpr_csv():
    a = 1
    assert a == 1
    pass

# @pytest.fixture
# def mock_conn():
#     return MockConnection()


# @pytest.fixture()
# def s3_mock_with_bucket():
#     with mock_aws():
#         s3 = boto3.client('s3', region_name='eu-west-2')
#
#         s3.create_bucket(Bucket='ingestion-bucket-neural-normalisers-new',
#                          CreateBucketConfiguration={
#                              'LocationConstraint': 'eu-west-2'}
#                          )
#         yield s3
#

# @pytest.fixture()
# def s3_mock_with_objects(s3_mock_with_bucket):
#     for i in range(10):
#         for table in tables:
#             fake_timestamp = f'{table}/2024-11-14T09:27:40.35701{i}.json'
#             s3_mock_with_bucket.put_object(Bucket='ingestion-bucket-neural-normalisers-new',
#                             Body=b'test_content',
#                             Key=fake_timestamp)
#     yield s3_mock_with_bucket
#
#
# expected_data = [
#             {
#                 "currency_id": 1,
#                 "currency_code": "GBP",
#                 "created_at": "2022-11-03T14:20:49",
#                 "amount": 100.0
#             },
#             {
#                 "currency_id": 2,
#                 "currency_code": "USD",
#                 "created_at": "2022-11-03T14:20:49",
#                 "amount": 200.0
#             }
#         ]
