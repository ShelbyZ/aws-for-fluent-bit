#!/bin/bash
# Delete the CloudFormation stack which created all the resources for running the integration test

set -e  # Exit on error

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

echo "=========================================="
echo "Starting deletion process for stack: ${STACK_NAME}"
echo "Region: ${AWS_REGION}"
echo "=========================================="

# Get the S3 bucket name from CloudFormation stack outputs
echo "Retrieving S3 bucket name from stack outputs..."
S3_BUCKET_NAME=$(aws cloudformation describe-stacks \
    --region "${AWS_REGION}" \
    --stack-name "${STACK_NAME}" \
    --query 'Stacks[0].Outputs[?OutputKey==`s3BucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

# Empty the S3 bucket if it exists
if [ -n "$S3_BUCKET_NAME" ] && [ "$S3_BUCKET_NAME" != "None" ]; then
    echo "Found S3 bucket: ${S3_BUCKET_NAME}"
    
    # Get the path to the cleanup script relative to this script's location
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    CLEANUP_SCRIPT="${SCRIPT_DIR}/../../scripts/pipeline/empty-s3-bucket.sh"
    
    if [ ! -f "$CLEANUP_SCRIPT" ]; then
        echo "ERROR: Cleanup script not found at: ${CLEANUP_SCRIPT}"
        echo "Attempting to empty bucket using AWS CLI directly..."
        
        # Fallback: try to empty bucket directly
        echo "Removing all object versions from bucket..."
        aws s3api list-object-versions \
            --bucket "${S3_BUCKET_NAME}" \
            --region "${AWS_REGION}" \
            --output json \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' | \
        while read -r line; do
            if [ "$line" != "{" ] && [ "$line" != "}" ] && [ -n "$line" ]; then
                aws s3api delete-objects \
                    --bucket "${S3_BUCKET_NAME}" \
                    --region "${AWS_REGION}" \
                    --delete "$line" 2>/dev/null || true
            fi
        done
        
        echo "Removing all delete markers from bucket..."
        aws s3api list-object-versions \
            --bucket "${S3_BUCKET_NAME}" \
            --region "${AWS_REGION}" \
            --output json \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' | \
        while read -r line; do
            if [ "$line" != "{" ] && [ "$line" != "}" ] && [ -n "$line" ]; then
                aws s3api delete-objects \
                    --bucket "${S3_BUCKET_NAME}" \
                    --region "${AWS_REGION}" \
                    --delete "$line" 2>/dev/null || true
            fi
        done
        
        echo "Bucket emptying completed (fallback method)"
    else
        echo "Emptying S3 bucket before stack deletion..."
        bash "$CLEANUP_SCRIPT" --bucket "${S3_BUCKET_NAME}" --region "${AWS_REGION}"
        echo "S3 bucket emptied successfully"
    fi
else
    echo "Warning: Could not retrieve S3 bucket name from stack outputs (got: '${S3_BUCKET_NAME}')"
    echo "Stack may not have an S3 bucket or stack doesn't exist yet"
fi

# Delete the CloudFormation stack
echo "=========================================="
echo "Deleting CloudFormation stack: ${STACK_NAME}"
echo "=========================================="
aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${AWS_REGION}"
echo "Stack deletion initiated successfully"
