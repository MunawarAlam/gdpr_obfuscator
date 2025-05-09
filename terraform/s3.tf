resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "ma-gdpr-ingestion-bucket"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "ma-gdpr-processed-bucket"
}
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_object" "ingestion_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "write_to_s3_lambda"
  source = "${path.module}/../src/ingestion.zip"
}

# resource "aws_s3_object" "pandas_layer" {
#   bucket = aws_s3_bucket.lambda_code_bucket.id
#   key = "pandas_layer"
#   source = "${path.module}/../layer.zip"
# }