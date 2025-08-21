# How to run on Engaging

Engaging uses a Slurm interface to schedule and manage jobs on MIT's Engaging HPC.

In order to run the `s3invsync` script, you'll simply want to call `sbatch your_script.sbatch` from anywhere where the file exists

Reference `engaging_sample_script.sbatch` to see how to run `s3invsync` gracefully against a given bucket. If failures exist, or only a specific directory in the entire S3 bucket is desired to be copied, be sure to evaluate the `PREFIX` related values in the script.

