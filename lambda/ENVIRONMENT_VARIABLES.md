# Lambda Environment Variables Configuration
# 
# Lambda functions don't use .env files. Instead, set these as environment variables
# in your Lambda function configuration in AWS Console or via Infrastructure as Code.
#
# Required Environment Variables for Lambda Functions:
#
# ==============================================================================
# COMPACTOR LAMBDA (lambda/compactor/compactor.py)
# ==============================================================================
# BUCKET=your_s3_bucket_name_here
# PARTIALS_PREFIX=results/daily
# OUT_PREFIX=analytics
#
# ==============================================================================
# PROCESSOR LAMBDA (lambda/processor/processor.py)
# ==============================================================================
# RAW_PREFIX=raw/
# RESULTS_PREFIX=results/
# PROCESSED_PREFIX=raw-processed/
#
# Note: Processor Lambda gets bucket name from S3 event trigger, so no BUCKET env var needed
#
# ==============================================================================
# AWS Credentials
# ==============================================================================
# Lambda functions should use IAM roles, NOT access keys.
# Attach an IAM role to your Lambda function with appropriate permissions:
# - S3 read/write permissions for the bucket
# - Firehose write permissions (if needed)
#
# ==============================================================================
# Example IAM Policy for Compactor Lambda:
# ==============================================================================
# {
#   \"Version\": \"2012-10-17\",
#   \"Statement\": [
#     {
#       \"Effect\": \"Allow\",
#       \"Action\": [
#         \"s3:GetObject\",
#         \"s3:PutObject\",
#         \"s3:ListBucket\"
#       ],
#       \"Resource\": [
#         \"arn:aws:s3:::your-bucket-name/*\",
#         \"arn:aws:s3:::your-bucket-name\"
#       ]
#     }
#   ]
# }
