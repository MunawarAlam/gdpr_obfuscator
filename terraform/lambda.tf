data "archive_file" "ingestion" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/ingestion.py"
  output_path = "${path.module}/../src/ingestion.zip"
}

# data "archive_file" "pandas_layer" {
#   type = "zip"
#   output_file_mode = "0666"
#   source_dir = "${path.module}/../layer"
#   output_path = "${path.module}/../layer.zip"
# }

resource "aws_lambda_layer_version" "pandas_layer" {
  layer_name = "pandas_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.pandas_layer.key
}


resource "aws_lambda_function" "ingestion" {
  function_name = "ingestion"
  handler = "ingestion.lambda_handler"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.ingestion_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = ["arn:aws:lambda:eu-north-1:336392948345:layer:AWSSDKPandas-Python312:16"]
  memory_size = 500
  environment {
    variables = {
    }
  }
}
