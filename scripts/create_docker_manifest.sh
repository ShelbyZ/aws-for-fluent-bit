#!/bin/bash

set -e

# Script to create and push Docker multi-architecture manifests
# Usage: ./create_docker_manifest.sh --aws_account <account> --aws_region <region> --ecr_repo <repo_name> --tag <tag>

# Function to display usage
usage() {
    echo "Usage: $0 --aws_account <account_id> --aws_region <region> --ecr_repo <repo_name> --tag <tag>"
    echo ""
    echo "Required flags:"
    echo "  --aws_account AWS account ID (e.g., 123456789012)"
    echo "  --aws_region  AWS region (e.g., us-west-2)"
    echo "  --ecr_repo    ECR repository name (e.g., amazon/aws-for-fluent-bit-test)"
    echo "  --tag         Tag for the manifest (e.g., latest, v1.0.0)"
    echo ""
    echo "Examples:"
    echo "  $0 --aws_account 123456789012 --aws_region us-west-2 --ecr_repo amazon/aws-for-fluent-bit-test --tag latest"
    echo "  $0 --aws_account 123456789012 --aws_region us-east-1 --ecr_repo my-repo --tag v1.0.0"
    exit 1
}

# Initialize variables
AWS_ACCOUNT=""
AWS_REGION=""
ECR_REPO=""
TAG=""

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
        --tag)
            TAG="$2"
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

if [ -z "$TAG" ]; then
    echo "Error: --tag is required"
    usage
fi

ECR_LOGIN_URL="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_ECR_REPO="${ECR_LOGIN_URL}/${ECR_REPO}"
AMD64_IMAGE="${FULL_ECR_REPO}:${TAG}-amd64"
ARM64_IMAGE="${FULL_ECR_REPO}:${TAG}-arm64"
MANIFEST_IMAGE="${FULL_ECR_REPO}:${TAG}"

echo "=== Docker Multi-Architecture Manifest Creator ==="
echo "AWS Account: ${AWS_ACCOUNT}"
echo "AWS Region: ${AWS_REGION}"
echo "ECR Repository: ${ECR_REPO}"
echo "Full ECR URL: ${FULL_ECR_REPO}"
echo "Tag: ${TAG}"
echo "AMD64 Image: ${AMD64_IMAGE}"
echo "ARM64 Image: ${ARM64_IMAGE}"
echo "Manifest Image: ${MANIFEST_IMAGE}"
echo ""

# Step 1: Login to ECR
echo "Step 1: Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_LOGIN_URL}
echo "✓ Successfully logged into ECR"
echo ""

# Step 2: Enable Docker CLI experimental features for manifest commands
echo "Step 2: Enabling Docker CLI experimental features..."
export DOCKER_CLI_EXPERIMENTAL=enabled
echo "✓ Docker CLI experimental features enabled"
echo ""

# Step 3: Check if both architecture images exist
echo "Step 3: Checking if both architecture images exist..."

# Function to check if image exists in ECR
check_image_exists() {
    local image_uri=$1
    echo "Checking if image exists: ${image_uri}"
    
    # Try to inspect the image manifest
    if docker manifest inspect ${image_uri} >/dev/null 2>&1; then
        echo "✓ Image exists: ${image_uri}"
        return 0
    else
        echo "✗ Image not found: ${image_uri}"
        return 1
    fi
}

AMD64_EXISTS=false
ARM64_EXISTS=false

if check_image_exists ${AMD64_IMAGE}; then
    AMD64_EXISTS=true
fi

if check_image_exists ${ARM64_IMAGE}; then
    ARM64_EXISTS=true
fi

if [ "$AMD64_EXISTS" = false ] && [ "$ARM64_EXISTS" = false ]; then
    echo "Error: Neither AMD64 nor ARM64 images exist. Cannot create manifest."
    exit 1
fi

echo ""

# Step 4: Create Docker manifest
echo "Step 4: Creating Docker manifest..."

MANIFEST_IMAGES=""
if [ "$AMD64_EXISTS" = true ]; then
    MANIFEST_IMAGES="${MANIFEST_IMAGES} ${AMD64_IMAGE}"
fi

if [ "$ARM64_EXISTS" = true ]; then
    MANIFEST_IMAGES="${MANIFEST_IMAGES} ${ARM64_IMAGE}"
fi

echo "Creating manifest: ${MANIFEST_IMAGE}"
echo "Including images:${MANIFEST_IMAGES}"

# Remove existing manifest if it exists (ignore errors)
docker manifest rm ${MANIFEST_IMAGE} 2>/dev/null || true

# Create the manifest
docker manifest create ${MANIFEST_IMAGE}${MANIFEST_IMAGES}
echo "✓ Successfully created manifest"
echo ""

# Step 5: Annotate manifest with architecture information
echo "Step 5: Annotating manifest with architecture information..."

if [ "$AMD64_EXISTS" = true ]; then
    echo "Annotating AMD64 image..."
    docker manifest annotate --arch amd64 ${MANIFEST_IMAGE} ${AMD64_IMAGE}
    echo "✓ AMD64 annotation added"
fi

if [ "$ARM64_EXISTS" = true ]; then
    echo "Annotating ARM64 image..."
    docker manifest annotate --arch arm64 ${MANIFEST_IMAGE} ${ARM64_IMAGE}
    echo "✓ ARM64 annotation added"
fi

echo ""

# Step 6: Inspect manifest (for verification)
echo "Step 6: Inspecting created manifest..."
docker manifest inspect ${MANIFEST_IMAGE}
echo "✓ Manifest inspection completed"
echo ""

# Step 7: Push manifest to ECR
echo "Step 7: Pushing manifest to ECR..."
docker manifest push ${MANIFEST_IMAGE}
echo "✓ Successfully pushed manifest to ECR"
echo ""

echo "=== Process completed successfully ==="
echo "Multi-architecture manifest created and pushed: ${MANIFEST_IMAGE}"

if [ "$AMD64_EXISTS" = true ] && [ "$ARM64_EXISTS" = true ]; then
    echo "✓ Manifest includes both AMD64 and ARM64 architectures"
elif [ "$AMD64_EXISTS" = true ]; then
    echo "⚠ Manifest includes only AMD64 architecture"
elif [ "$ARM64_EXISTS" = true ]; then
    echo "⚠ Manifest includes only ARM64 architecture"
fi
