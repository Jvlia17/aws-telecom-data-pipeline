
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.telecom_data.id
  key    = "scripts/bronze_to_silver.py"

  source = "${path.module}/../scripts/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../scripts/bronze_to_silver.py")

  content_type = "text/x-python"
}
