#!/bin/bash
# =============================================================================
# pull_from_vm.sh - Pull completed SEC filings from VM to OneDrive
# =============================================================================
# Run this on your Mac to transfer completed files from the VM.
# The VM will automatically resume scanning once storage is freed.
# =============================================================================

set -e

# Configuration - EDIT THESE
VM_USER="zade"
VM_HOST="10.20.5.30"
VM_STAGE_DIR="~/edgar_tmp/stage"

# Local OneDrive destination (already fixed in scan.py)
ONEDRIVE_DIR="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-UniversityofTulsa/NSF-BSF Precautions - crypto10k"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "SEC Crypto Analysis - VM Pull Script"
echo "=============================================="

# Check if OneDrive folder exists
if [ ! -d "$ONEDRIVE_DIR" ]; then
    echo -e "${RED}ERROR: OneDrive folder not found:${NC}"
    echo "$ONEDRIVE_DIR"
    echo "Make sure OneDrive is syncing and the folder exists."
    exit 1
fi

# Check SSH connectivity
echo -e "${YELLOW}Checking VM connectivity...${NC}"
if ! ssh -o ConnectTimeout=5 -o BatchMode=yes "${VM_USER}@${VM_HOST}" "echo connected" &>/dev/null; then
    echo -e "${RED}ERROR: Cannot connect to VM at ${VM_USER}@${VM_HOST}${NC}"
    echo "Check that:"
    echo "  1. VM is running"
    echo "  2. SSH key is set up (ssh-copy-id ${VM_USER}@${VM_HOST})"
    echo "  3. Network is accessible"
    exit 1
fi
echo -e "${GREEN}Connected to VM${NC}"

# Get list of completed CIK folders on VM
echo -e "${YELLOW}Finding completed CIK folders on VM...${NC}"
COMPLETED_DIRS=$(ssh "${VM_USER}@${VM_HOST}" "
    cd ${VM_STAGE_DIR} 2>/dev/null || exit 0
    for dir in */; do
        if [ -f \"\${dir}COMPLETE\" ]; then
            echo \"\${dir%/}\"
        fi
    done
")

if [ -z "$COMPLETED_DIRS" ]; then
    echo -e "${GREEN}No completed folders to transfer.${NC}"

    # Show VM status
    echo ""
    echo "VM Status:"
    ssh "${VM_USER}@${VM_HOST}" "python3 ~/sec_crypto/VMscan.py --status 2>/dev/null || echo 'Could not get status'"
    exit 0
fi

# Count folders
FOLDER_COUNT=$(echo "$COMPLETED_DIRS" | wc -l | tr -d ' ')
echo -e "${GREEN}Found ${FOLDER_COUNT} completed folder(s) to transfer${NC}"

# Transfer each folder
TRANSFERRED=0
FAILED=0

for CIK_DIR in $COMPLETED_DIRS; do
    echo -e "${YELLOW}Transferring: ${CIK_DIR}${NC}"

    # Create local directory
    mkdir -p "${ONEDRIVE_DIR}/${CIK_DIR}"

    # rsync the folder (excluding COMPLETE and .STAGING markers)
    if rsync -av --progress \
        --exclude='COMPLETE' \
        --exclude='.STAGING' \
        "${VM_USER}@${VM_HOST}:${VM_STAGE_DIR}/${CIK_DIR}/" \
        "${ONEDRIVE_DIR}/${CIK_DIR}/"; then

        # Successfully transferred - delete from VM
        echo -e "${GREEN}Deleting ${CIK_DIR} from VM...${NC}"
        ssh "${VM_USER}@${VM_HOST}" "rm -rf ${VM_STAGE_DIR}/${CIK_DIR}"
        ((TRANSFERRED++))
    else
        echo -e "${RED}Failed to transfer ${CIK_DIR}${NC}"
        ((FAILED++))
    fi
done

echo ""
echo "=============================================="
echo -e "${GREEN}Transfer complete!${NC}"
echo "  Transferred: ${TRANSFERRED}"
echo "  Failed: ${FAILED}"
echo "=============================================="

# Show VM status after transfer
echo ""
echo "VM Status after transfer:"
ssh "${VM_USER}@${VM_HOST}" "python3 ~/sec_crypto/VMscan.py --status 2>/dev/null || echo 'Could not get status'"
