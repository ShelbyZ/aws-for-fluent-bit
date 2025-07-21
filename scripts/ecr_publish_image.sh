#!/bin/bash

set -e

# Script to login to ECR, tag images, and create multi-arch manifests
# Usage: ./ecr_publish_manifest.sh --aws_account <account> --aws_region <region> --ecr_repo <repo_name> --source <source_tag>

# Function to display usage
usage() {
    echo "Usage: $0 --aws_account <account_id> --aws_region <region> --ecr_repo <repo_name> --source <source_tag>"
    echo ""
    echo "Required flags:"
    echo "  --aws_account AWS account ID (e.g., 123456789012)"
    echo "  --aws_region  AWS region (e.g., us-west-2)"
    echo "  --ecr_repo    ECR repository name (e.g., amazon/aws-for-fluent-bit)"
    echo "  --source      Source tag for the images to be retagged"
    echo ""
    echo "Examples:"
    echo "  $0 --aws_account 123456789012 --aws_region us-west-2 --ecr_repo amazon/aws-for-fluent-bit --source v1.0.0-build"
    echo "  $0 --source v1.0.0-build --aws_account 123456789012 --aws_region us-east-1 --ecr_repo my-repo"
    exit 1
}

# Initialize variables
AWS_ACCOUNT=""
AWS_REGION=""
ECR_REPO=""
SOURCE_TAG=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --aws_account)
            AWS_ACCOUNT="$2"
            shift 2
            ;;
        --aws_region)
            AWS_REGION="$2"
            shift 2
            ;;
        --ecr_repo)
            ECR_REPO="$2"
            shift 2
            ;;
        --source)
            SOURCE_TAG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: Unknown option $1"
            usage
            ;;
    esac
done

# Validate required parameters
if [ -z "$AWS_ACCOUNT" ]; then
    echo "Error: --aws_account is required"
    usage
fi

if [ -z "$AWS_REGION" ]; then
    echo "Error: --aws_region is required"
    usage
fi

if [ -z "$ECR_REPO" ]; then
    echo "Error: --ecr_repo is required"
    usage
fi

if [ -z "$SOURCE_TAG" ]; then
    echo "Error: --source is required"
    usage
fi

LOCAL_SOURCE_IMAGE="${ECR_REPO}:${SOURCE_TAG}"

# Get architecture from ${ECR_REPO}:${SOURCE_TAG} image
echo "Detecting architecture from ${LOCAL_SOURCE_IMAGE} image..."
ARCHITECTURE=$(docker inspect --format='{{.Architecture}}' ${LOCAL_SOURCE_IMAGE} 2>/dev/null)

if [ -z "$ARCHITECTURE" ]; then
    echo "Error: Could not detect architecture from amazon/aws-for-fluent-bit image"
    echo "Make sure the image exists locally or pull it first"
    exit 1
fi

ECR_LOGIN_URL="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_ECR_REPO="${ECR_LOGIN_URL}/${ECR_REPO}"
TARGET_IMAGE="${FULL_ECR_REPO}-bug-bash:${SOURCE_TAG}-${ARCHITECTURE}"

echo "=== ECR Multi-Architecture Manifest Publisher ==="
echo "AWS Account: ${AWS_ACCOUNT}"
echo "AWS Region: ${AWS_REGION}"
echo "ECR Repository: ${ECR_REPO}"
echo "Full ECR URL: ${FULL_ECR_REPO}"
echo "Source Tag: ${SOURCE_TAG}"
echo "Target Tag: ${TARGET_TAG}"
echo "Detected Architecture: ${ARCHITECTURE}"
echo ""

# Step 1: Login to ECR
echo "Step 1: Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_LOGIN_URL}
echo "✓ Successfully logged into ECR"
echo ""

# Step 2: Tag local image with ECR repo and target tag
echo "Step 2: Tagging local image with ECR repository..."

echo "Checking for local source image: ${LOCAL_SOURCE_IMAGE}"

# Check if local source image exists
if docker image inspect "${LOCAL_SOURCE_IMAGE}" >/dev/null 2>&1; then
    echo "✓ Found local source image: ${LOCAL_SOURCE_IMAGE}"
    
    # Tag the local image with ECR repo and target tag
    echo "Tagging ${LOCAL_SOURCE_IMAGE} -> ${TARGET_IMAGE}"
    docker tag ${LOCAL_SOURCE_IMAGE} ${TARGET_IMAGE}
    echo "✓ Successfully tagged image"
    
    echo ""
    echo "Step 3: Pushing image to ECR..."
    
    # Push the tagged image to ECR
    echo "Pushing ${TARGET_IMAGE}"
    docker push ${TARGET_IMAGE}
    echo "✓ Successfully pushed image to ECR"
else
    echo "✗ Local source image not found: ${LOCAL_SOURCE_IMAGE}"
    echo "Make sure the image exists locally with the specified source tag"
    exit 1
fi

echo ""
echo "=== Process completed successfully ==="
