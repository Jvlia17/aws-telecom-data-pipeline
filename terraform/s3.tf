
resource "aws_s3_bucket" "telecom_data" {
  bucket = "telecom-data-pipeline-jvlia17"

  tags = {
    Name        = "Telecom Data Pipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "telecom_data" {
  bucket = aws_s3_bucket.telecom_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "telecom_data" {
  bucket = aws_s3_bucket.telecom_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
