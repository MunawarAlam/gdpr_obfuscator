
import os
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("PATH:", os.environ.get('PATH'))

from src.ingestion import *
import pytest
import boto3
import pandas as pd
import numpy as np

##
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

def test_pii_field_s_provided_initial_input():
    input = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": []}'
    output = set_initial_input(input, gdpr_init)
    assert output == False

def test_object_not_exist(caplog):
    input = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/No_Students_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    set_initial_input(input, gdpr_init)

    with caplog.at_level(logging.INFO):
        response = object_exist_check(gdpr_init)
        assert "No Obfuscator file found from S3" in caplog.text

def test_obfuscator_completed_successfully_on_pii_fields(caplog):
    input = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    set_initial_input(input, gdpr_init)
    with caplog.at_level(logging.INFO):
        response = gdpr_csv(gdpr_init)
        assert "Obfuscator file created in S3" in caplog.text

def test_data_converted_based_on_pii():
    expected_output = [{'Student_ID': 'S1000', 'First_Name': '**********', 'Last_Name': 'Williams', 'Email': 'student0@university.com', 'Gender': 'Female', 'Age': 22, 'Department': 'Engineering', 'Attendance (%)': 47.50823, 'Participation_Score': 5.188714154, 'Projects_Score': 50.79, 'Total_Score': 56.09, 'Grade': 'F', 'Study_Hours_per_Week': 14.0225, 'Extracurricular_Activities': 'No', 'Internet_Access_at_Home': 'Yes', 'Parent_Education_Level': 'High School', 'Family_Income_Level': 'Low', 'Stress_Level (1-10)': 5, 'Sleep_Hours_per_Night': 4.7, 'Sleep_Hours_per_Night_Entier': 5, 'Country': 'US'},
                       {'Student_ID': 'S1001', 'First_Name': '**********', 'Last_Name': 'Brown', 'Email': 'student1@university.com', 'Gender': 'Male', 'Age': 18, 'Department': 'Engineering', 'Attendance (%)': 45.62664, 'Participation_Score': 4.855225312, 'Projects_Score': 48.37, 'Total_Score': 50.64, 'Grade': 'A', 'Study_Hours_per_Week': 12.66, 'Extracurricular_Activities': 'No', 'Internet_Access_at_Home': 'No', 'Parent_Education_Level': None, 'Family_Income_Level': 'Low', 'Stress_Level (1-10)': 4, 'Sleep_Hours_per_Night': 9.0, 'Sleep_Hours_per_Night_Entier': 9, 'Country': 'Japan'}]

    init_input = '{"file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv","pii_fields": ["first_Name", "email_address"]}'
    set_initial_input(init_input, gdpr_init)
    gdpr_csv(gdpr_init)
    #
    s3_object = gdpr_init.s3_client.get_object(
        Bucket='ma-gdpr-processed-bucket',
        Key='new_data/Students_Grading_Dataset.csv'
    )
    #
    body = pd.read_csv(s3_object['Body'])
    body = body.head(2)
    body.reset_index(drop=True, inplace=True)
    output = body.replace([np.nan], [None], regex=False).to_dict(orient='records')
    assert  output == expected_output
    #
def test_lambda_handler_gdpr_s3_file_created(caplog):
    expected_output = [{'Student_ID': 'S1000', 'First_Name': '**********', 'Last_Name': 'Williams', 'Email': 'student0@university.com', 'Gender': 'Female', 'Age': 22, 'Department': 'Engineering', 'Attendance (%)': 47.50823, 'Participation_Score': 5.188714154, 'Projects_Score': 50.79, 'Total_Score': 56.09, 'Grade': 'F', 'Study_Hours_per_Week': 14.0225, 'Extracurricular_Activities': 'No', 'Internet_Access_at_Home': 'Yes', 'Parent_Education_Level': 'High School', 'Family_Income_Level': 'Low', 'Stress_Level (1-10)': 5, 'Sleep_Hours_per_Night': 4.7, 'Sleep_Hours_per_Night_Entier': 5, 'Country': 'US'},
                       {'Student_ID': 'S1001', 'First_Name': '**********', 'Last_Name': 'Brown', 'Email': 'student1@university.com', 'Gender': 'Male', 'Age': 18, 'Department': 'Engineering', 'Attendance (%)': 45.62664, 'Participation_Score': 4.855225312, 'Projects_Score': 48.37, 'Total_Score': 50.64, 'Grade': 'A', 'Study_Hours_per_Week': 12.66, 'Extracurricular_Activities': 'No', 'Internet_Access_at_Home': 'No', 'Parent_Education_Level': None, 'Family_Income_Level': 'Low', 'Stress_Level (1-10)': 4, 'Sleep_Hours_per_Night': 9.0, 'Sleep_Hours_per_Night_Entier': 9, 'Country': 'Japan'}]

    input = {
        "file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv",
        "pii_fields": ["first_Name", "email_address"]
    }
    lambda_handler(input, None)

    s3_object = gdpr_init.s3_client.get_object(
        Bucket='ma-gdpr-processed-bucket',
        Key='new_data/Students_Grading_Dataset.csv'
    )
    #
    body = pd.read_csv(s3_object['Body'])
    body = body.head(2)
    body.reset_index(drop=True, inplace=True)
    output = body.replace([np.nan], [None], regex=False).to_dict(orient='records')
    assert  output == expected_output
    #

def test_lambda_handler_gdpr_s3_processed_obfuscator_successfully(caplog):
    input = {
        "file_to_obfuscate": "s3://ma-gdpr-ingestion-bucket/new_data/Students_Grading_Dataset.csv",
        "pii_fields": ["first_Name", "email_address"]
    }
    lambda_handler(input, None)

    s3_object = gdpr_init.s3_client.get_object(
        Bucket='ma-gdpr-processed-bucket',
        Key='new_data/Students_Grading_Dataset.csv'
    )
    #
    with caplog.at_level(logging.INFO):
        response = lambda_handler(input, None)
        assert "Successfully processed obfuscator" in caplog.text
