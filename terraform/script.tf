data "archive_file" "telecom_package" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/telecom_pipeline.zip"

  excludes = [
    "**/__pycache__/**",
    "**/*.pyc",
  ]
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.telecom_data.id
  key    = "scripts/bronze_to_silver.py"

  source = "${path.module}/../scripts/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../scripts/bronze_to_silver.py")

  content_type = "text/x-python"
}

resource "aws_s3_object" "glue_package" {
  bucket = aws_s3_bucket.telecom_data.id
  key    = "scripts/telecom_pipeline.zip"

  source = data.archive_file.telecom_package.output_path
  etag   = data.archive_file.telecom_package.output_md5

  content_type = "application/zip"
}


resource "aws_s3_object" "silver_to_gold_script" {
  bucket = "telecom-data-pipeline-jvlia17"
  key    = "scripts/silver_to_gold.py"
  source = "${path.module}/../scripts/silver_to_gold.py"
  etag   = filemd5("${path.module}/../scripts/silver_to_gold.py")
}
