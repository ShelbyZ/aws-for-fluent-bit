#!/bin/bash
# Delete the CloudFormation stack which created all the resources for running the integration test
ARCHITECTURE=$(uname -m | tr '_' '-')
# For arm, uname evaluates to 'aarch64' but everywhere else in the pipline
# we use 'arm64'
if [ "$ARCHITECTURE" = "aarch64" ]; then
    ARCHITECTURE="arm64"
fi

# Default BUILD_VERSION to 2 if not set
BUILD_VERSION=${BUILD_VERSION:-2}

STACK_NAME="integ-test-fluent-bit-${ARCHITECTURE}-V${BUILD_VERSION}"
AWS_REGION="${AWS_REGION:-us-west-2}"

# Get the S3 bucket name from CloudFormation stack outputs
echo "Retrieving S3 bucket name from stack outputs..."
S3_BUCKET_NAME=$(aws cloudformation describe-stacks \
    --region "${AWS_REGION}" \
    --stack-name "${STACK_NAME}" \
    --query 'Stacks[0].Outputs[?OutputKey==`s3BucketName`].OutputValue' \
    --output text 2>/dev/null)

# Empty the S3 bucket if it exists
if [ -n "$S3_BUCKET_NAME" ]; then
    echo "Found S3 bucket: ${S3_BUCKET_NAME}"
    
    # Get the path to the cleanup script relative to this script's location
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    CLEANUP_SCRIPT="${SCRIPT_DIR}/../../scripts/pipeline/empty-s3-bucket.sh"
    
    echo "Emptying S3 bucket before stack deletion..."
    "$CLEANUP_SCRIPT" --bucket "${S3_BUCKET_NAME}" --region "${AWS_REGION}"
else
    echo "Warning: Could not retrieve S3 bucket name from stack outputs"
fi

# Delete the CloudFormation stack
echo "Deleting CloudFormation stack: ${STACK_NAME}"
aws cloudformation delete-stack --stack-name "${STACK_NAME}"
