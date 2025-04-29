resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "gdpr_ingestion_bucket"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "gdpr_processed_bucket"
}
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_object" "ingestion_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "write_to_s3_lambda"
  source = "${path.module}/../src/ingestion.zip"
}
