#!/bin/bash
# Script to empty an S3 bucket by removing all objects and versions
# This is useful before deleting CloudFormation stacks that contain S3 buckets

set -e

# Default values
BUCKET_NAME=""
REGION="us-west-2"
DRY_RUN=false

# Function to display usage information
usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Empty an S3 bucket by removing all objects and versions."
  echo ""
  echo "Options:"
  echo "  -h, --help                 Display this help message"
  echo "  -b, --bucket BUCKET        S3 bucket name (required)"
  echo "  -r, --region REGION        AWS region (default: us-west-2)"
  echo "  -d, --dry-run              Show what would be deleted without actually deleting"
  echo ""
  echo "Examples:"
  echo "  $0 --bucket my-test-bucket"
  echo "  $0 -b my-test-bucket -r us-east-1"
  echo "  $0 --bucket my-test-bucket --dry-run"
  exit 1
}

# Function to parse command line arguments
parse_args() {
  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
      -h|--help)
        usage
        ;;
      -b|--bucket)
        BUCKET_NAME="$2"
        shift 2
        ;;
      -r|--region)
        REGION="$2"
        shift 2
        ;;
      -d|--dry-run)
        DRY_RUN=true
        shift
        ;;
      *)
        echo "Unknown option: $1"
        usage
        ;;
    esac
  done
}

# Function to handle errors
error_exit() {
  echo "ERROR: $1" >&2
  exit 1
}

# Function to log information
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Function to validate arguments
validate_args() {
  [[ -z "${BUCKET_NAME}" ]] && error_exit "Bucket name is required. Use -b or --bucket to specify it."
  [[ -z "${REGION}" ]] && error_exit "Region cannot be empty."
}

# Function to check if bucket exists
check_bucket_exists() {
  log "Checking if bucket exists: ${BUCKET_NAME}"
  
  if ! aws s3 ls "s3://${BUCKET_NAME}" --region "${REGION}" 2>/dev/null; then
    error_exit "Bucket '${BUCKET_NAME}' does not exist or is not accessible in region '${REGION}'"
  fi
  
  log "Bucket exists and is accessible"
}

# Function to delete all current objects
delete_current_objects() {
  log "Checking for current objects in bucket..."
  
  local object_count=$(aws s3 ls "s3://${BUCKET_NAME}" --recursive --region "${REGION}" 2>/dev/null | wc -l)
  
  if [ "$object_count" -eq 0 ]; then
    log "No current objects found in bucket"
    return 0
  fi
  
  log "Found ${object_count} current objects"
  
  if [ "$DRY_RUN" = true ]; then
    log "[DRY RUN] Would delete ${object_count} current objects"
    aws s3 ls "s3://${BUCKET_NAME}" --recursive --region "${REGION}" | head -20
    if [ "$object_count" -gt 20 ]; then
      log "[DRY RUN] ... and $((object_count - 20)) more objects"
    fi
  else
    log "Deleting current objects..."
    aws s3 rm "s3://${BUCKET_NAME}" --recursive --region "${REGION}"
    log "Current objects deleted successfully"
  fi
}

# Function to delete all object versions (for versioned buckets)
delete_object_versions() {
  log "Checking for object versions..."
  
  local next_token=""
  local total_versions_deleted=0
  local total_markers_deleted=0
  local batch_num=0
  
  while true; do
    batch_num=$((batch_num + 1))
    log "Processing batch ${batch_num}..."
    
    # Build the command with optional next-token
    local list_cmd="aws s3api list-object-versions --bucket ${BUCKET_NAME} --region ${REGION} --max-items 1000 --output json"
    if [ -n "$next_token" ]; then
      list_cmd="${list_cmd} --starting-token ${next_token}"
    fi
    
    local versions_json=$(eval "$list_cmd" 2>/dev/null)
    
    if [ -z "$versions_json" ] || [ "$versions_json" = "null" ]; then
      if [ "$batch_num" -eq 1 ]; then
        log "No object versions found (bucket may not have versioning enabled)"
      fi
      break
    fi
    
    local version_count=$(echo "$versions_json" | jq -r '.Versions // [] | length')
    local delete_marker_count=$(echo "$versions_json" | jq -r '.DeleteMarkers // [] | length')
    local batch_total=$((version_count + delete_marker_count))
    
    if [ "$batch_total" -eq 0 ]; then
      log "No more object versions or delete markers found"
      break
    fi
    
    log "Batch ${batch_num}: Found ${version_count} object versions and ${delete_marker_count} delete markers"
    
    if [ "$DRY_RUN" = true ]; then
      log "[DRY RUN] Would delete ${version_count} object versions and ${delete_marker_count} delete markers in this batch"
      if [ "$batch_num" -eq 1 ]; then
        echo "$versions_json" | jq -r '.Versions[]? | "\(.Key) (Version: \(.VersionId))"' | head -10
        if [ "$version_count" -gt 10 ]; then
          log "[DRY RUN] ... and $((version_count - 10)) more versions in this batch"
        fi
      fi
    else
      # Delete versions in batches
      if [ "$version_count" -gt 0 ]; then
        log "Deleting ${version_count} object versions from batch ${batch_num}..."
        local delete_payload=$(echo "$versions_json" | jq '{Objects: [.Versions[]? | {Key: .Key, VersionId: .VersionId}]}')
        
        if [ "$delete_payload" != "null" ] && [ -n "$delete_payload" ]; then
          aws s3api delete-objects \
            --bucket "${BUCKET_NAME}" \
            --delete "$delete_payload" \
            --region "${REGION}" > /dev/null
          total_versions_deleted=$((total_versions_deleted + version_count))
          log "Deleted ${version_count} object versions from batch ${batch_num}"
        fi
      fi
      
      # Delete delete markers in batches
      if [ "$delete_marker_count" -gt 0 ]; then
        log "Deleting ${delete_marker_count} delete markers from batch ${batch_num}..."
        local delete_markers_payload=$(echo "$versions_json" | jq '{Objects: [.DeleteMarkers[]? | {Key: .Key, VersionId: .VersionId}]}')
        
        if [ "$delete_markers_payload" != "null" ] && [ -n "$delete_markers_payload" ]; then
          aws s3api delete-objects \
            --bucket "${BUCKET_NAME}" \
            --delete "$delete_markers_payload" \
            --region "${REGION}" > /dev/null
          total_markers_deleted=$((total_markers_deleted + delete_marker_count))
          log "Deleted ${delete_marker_count} delete markers from batch ${batch_num}"
        fi
      fi
    fi
    
    # Check for next token to continue pagination
    next_token=$(echo "$versions_json" | jq -r '.NextToken // empty')
    if [ -z "$next_token" ]; then
      log "No more batches to process"
      break
    fi
    
    log "More versions found, continuing to next batch..."
  done
  
  if [ "$DRY_RUN" = false ] && [ "$batch_num" -gt 1 ]; then
    log "Total deleted across all batches: ${total_versions_deleted} versions and ${total_markers_deleted} delete markers"
  fi
}

# Function to verify bucket is empty
verify_empty() {
  if [ "$DRY_RUN" = true ]; then
    log "[DRY RUN] Skipping verification"
    return 0
  fi
  
  log "Verifying bucket is empty..."
  
  local remaining_objects=$(aws s3 ls "s3://${BUCKET_NAME}" --recursive --region "${REGION}" 2>/dev/null | wc -l)
  
  if [ "$remaining_objects" -eq 0 ]; then
    log "Bucket is now empty"
  else
    log "Warning: ${remaining_objects} objects still remain in bucket"
  fi
}

# Main function
main() {
  log "Starting S3 bucket cleanup process"
  
  # Parse command line arguments
  parse_args "$@"
  
  # Validate arguments
  validate_args
  
  if [ "$DRY_RUN" = true ]; then
    log "Running in DRY RUN mode - no changes will be made"
  fi
  
  # Check if bucket exists
  check_bucket_exists
  
  # Delete current objects
  delete_current_objects
  
  # Delete object versions (if versioning is enabled)
  delete_object_versions
  
  # Verify bucket is empty
  verify_empty
  
  if [ "$DRY_RUN" = true ]; then
    log "DRY RUN completed - no changes were made"
  else
    log "Bucket cleanup completed successfully"
  fi
}

# Execute the main function with all script arguments
main "$@"
