
resource "aws_glue_job" "telecom_bronze_to_silver" {
  name     = "telecom-bronze-to-silver"
  role_arn = "arn:aws:iam::152174417047:role/AWSGlueServiceRole-telecom-pipeline"

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 5

  command {
    name            = "glueetl"
    script_location = "s3://telecom-data-pipeline-jvlia17/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "false"
    "--TempDir"                          = "s3://telecom-data-pipeline-jvlia17/tmp/"
  }

  tags = {
    Project     = "telecom-data-pipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_s3_object.glue_script
  ]
}
