#!/bin/bash
#SBATCH --job-name=zarr_specific_sync
#SBATCH --partition=ou_bcs_normal
#SBATCH --time=11:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --output=slurm-%A.out
#SBATCH --error=slurm-%A.err

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
JOB_LABEL="job_${TIMESTAMP}"
# Example PREFIX="zarr/caed99b5-c90c-48bd-8c99-6f338f78570d"

# echo "[$JOB_LABEL] Syncing specific S3 prefix: ${PREFIX}"

# Stagger startup
sleep $((RANDOM % 10 + 1))

# Quota check
echo "[$JOB_LABEL] Disk quota:"
quota -s 2>/dev/null || echo "Quota check failed (non-critical)"

# Env + logging
export PATH="$HOME/.local/bin:$PATH"
export RUST_LOG=s3invsync=info

echo "[$JOB_LABEL] Using s3invsync from: $(which s3invsync)"
s3invsync --version

# Sync the specific zarr prefix -- you could simply remove the --path-filter and PREFIX if you wanted to run for the entire bucket
stdbuf -oL -eL s3invsync --path-filter "${PREFIX}" --allow-new-nonempty --ignore-errors all \
    s3://linc-brain-mit-prod-us-east-2/linc-brain-mit-prod-us-east-2/production-configuration/ \
    /orcd/data/linc/001/s3lincbrain/ &

S3INVSYNC_PID=$!

# Graceful shutdown -- done so that if the length of the sync lasts longer than the maximum amount of time a job can handle, the sync stops gracefully
trap 'echo "[$JOB_LABEL] Caught SIGTERM. Forwarding to s3invsync (PID $S3INVSYNC_PID)..."; kill -SIGINT "$S3INVSYNC_PID"; wait "$S3INVSYNC_PID"; exit' TERM

wait "$S3INVSYNC_PID"

echo "[$JOB_LABEL] Sync for ${PREFIX} completed"