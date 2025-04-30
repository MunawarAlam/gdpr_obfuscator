resource "aws_iam_role" "lambda_role" {
  name = "project_lambda_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*", "${aws_s3_bucket.processed_bucket.arn}/*"]
    }
}

data "aws_iam_policy_document" "bucket_document" {
  statement {
    actions = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}", "${aws_s3_bucket.processed_bucket.arn}"]
    }
}

resource "aws_iam_policy" "s3_policy" {
  name = "lambda_s3_policy"
  policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "bucket_policy" {
  name = "bucket_document_policy"
  policy = data.aws_iam_policy_document.bucket_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "bucket_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.bucket_policy.arn
}

resource "aws_iam_role" "iam_for_sfn" {
  name_prefix        = "role-for-sfn-"
  assume_role_policy = <<EOF
{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "states.amazonaws.com",
                        "events.amazonaws.com",
                        "scheduler.amazonaws.com"
                    ]
                }
            }
        ]
    }
EOF
}

resource "aws_iam_role" "eventbridge_scheduler_iam_role" {
  name_prefix         = "eventbridge-scheduler-role-"
  assume_role_policy  = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "scheduler.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}