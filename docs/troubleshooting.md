# Troubleshooting Guide

## Common Issues & Fixes

---

### 1. CloudFormation Stack Fails to Create

**Symptom:** Stack status shows `CREATE_FAILED` or `ROLLBACK_COMPLETE`

**Common causes:**
- **IAM permissions:** Your AWS user needs permissions to create IAM roles, VPCs, EC2 instances, S3 buckets, Glue resources. Use an admin account or ensure all required permissions are granted.
- **S3 bucket name conflict:** The bucket name `nse-bigdata-<account-id>-<region>` must be globally unique. If it already exists (from a previous run), delete the old bucket first.
- **Service limits:** Check if you've hit EC2 instance limits or VPC limits in your region.

**Fix:** Check the **Events** tab in CloudFormation for the specific error message. Delete the failed stack and retry after fixing the root cause.

---

### 2. Cannot SSH into Bastion Host

**Symptom:** `Connection timed out` or `Permission denied`

**Fixes:**
- Verify the `SSHLocation` parameter matches your public IP. Check your IP at [whatismyip.com](https://whatismyip.com) and use `<your-ip>/32`.
- Ensure you downloaded the private key correctly from SSM Parameter Store:
  ```bash
  aws ssm get-parameter \
    --name /ec2/keypair/<KEY-PAIR-ID> \
    --with-decryption \
    --query Parameter.Value \
    --output text > workshop-keypair.pem
  chmod 400 workshop-keypair.pem
  ```
- Use the correct username: `ec2-user` (not `root` or `ubuntu`)
- Check that the Bastion Host is in `running` state in the EC2 console

---

### 3. NSE Data Not in S3 Bucket

**Symptom:** `aws s3 ls s3://<bucket>/NSE/` returns empty

**Cause:** The UserData script may still be running (it syncs ~2GB of data).

**Fix:**
- SSH into the Bastion Host and check: `cat ~/setup-complete.txt`
- If the file doesn't exist, the sync is still in progress. Wait a few minutes.
- Check the UserData log: `cat /var/log/cloud-init-output.log`
- Manual sync: `aws s3 sync s3://dbsbdclassnv/indiastockex/raw/ s3://<your-bucket>/NSE/`

---

### 4. Glue Crawler Shows No Tables

**Symptom:** Crawler ran but no tables appear in `nsedatabase`

**Fixes:**
- Verify data exists in S3: `aws s3 ls s3://<bucket>/NSE/`
- Check the crawler's S3 target path matches exactly: `s3://<bucket>/NSE/`
- Re-run the crawler manually: Glue Console → Crawlers → Select → Run crawler
- Check the crawler's IAM role has S3 read permissions

---

### 5. Athena Queries Fail

**Symptom:** `HIVE_METASTORE_ERROR` or `Access Denied`

**Fixes:**
- Select the correct workgroup: `<StackName>-Workgroup`
- Ensure the Athena results bucket exists and is accessible
- Select `nsedatabase` in the left panel before running queries
- If table names have special characters, wrap them in backticks: `` `table_name` ``

---

### 6. EMR Cluster Stuck in "Starting"

**Symptom:** Cluster doesn't reach "Waiting" status after 20+ minutes

**Fixes:**
- Check the cluster's **Events** tab for error messages
- Verify the EMR Service Role and EC2 Instance Profile ARNs from the stack Outputs
- Ensure the selected subnet has internet access (public subnet with IGW)
- Check that the key pair name matches exactly

---

### 7. Cannot Access Hue Web Interface

**Symptom:** Browser shows "connection refused" for `http://<master-dns>:8888`

**Fixes:**
- Ensure the SSH tunnel is running: `ssh -i keypair.pem -N -D 8157 hadoop@<master-dns>`
- Verify FoxyProxy is enabled and configured (SOCKS5, localhost:8157)
- Check that port 8888 is open in the EMR Master security group
- Try accessing directly if your IP is in the security group's allowed range

---

### 8. s3-dist-cp Fails on EMR

**Symptom:** `Access Denied` or `No such file` errors

**Fixes:**
- Verify AWS CLI is configured on the Master node: `aws configure`
- Check the S3 path is correct: `aws s3 ls s3://<bucket>/NSE/`
- Ensure the EMR EC2 role has S3 access permissions
- Try with full path: `s3-dist-cp --src s3://<bucket>/NSE/ --dest /home/hadoop/NSEraw`

---

### 9. Hive Cannot Find Glue Tables

**Symptom:** `Table not found` in Hive editor

**Fixes:**
- Confirm you checked "Use for Hive table metadata" when creating the EMR cluster
- In Hive, don't use double quotes around database/table names (use backticks if needed)
- Run `SHOW DATABASES;` to verify `nsedatabase` is visible
- Run `USE nsedatabase; SHOW TABLES;` to list available tables

---

### 10. MapReduce Jobs Fail on EMR

**Symptom:** Jobs fail with `Container killed by YARN` or `OutOfMemory`

**Fixes:**
- Increase memory: `SET mapreduce.map.memory.mb=2048;`
- Switch to Tez for better memory management: `SET hive.execution.engine=tez;`
- Check Resource Manager at `http://<master-dns>:8088` for detailed error logs
- Consider using larger instance types for EMR nodes

---

### 11. QuickSight Cannot Access S3 Data

**Symptom:** Dataset creation fails with permission errors

**Fixes:**
- In QuickSight settings, grant access to your S3 bucket
- Update the manifest file (`config/quicksight-manifest.json`) with your actual bucket name
- Ensure the data files are in CSV format with consistent delimiters
- Try creating the dataset using Athena as the source instead of direct S3

---

## Cleanup Issues

### Stack Deletion Fails

**Symptom:** `DELETE_FAILED` status

**Common cause:** S3 buckets with data cannot be auto-deleted.

**Fix:**
1. Empty the S3 buckets first:
   ```bash
   aws s3 rm s3://<bucket-name> --recursive
   ```
2. Terminate any manually created EMR clusters
3. Retry the stack deletion

---

## Getting Help

- Check [AWS EMR Documentation](https://docs.aws.amazon.com/emr/)
- Check [AWS Athena Documentation](https://docs.aws.amazon.com/athena/)
- Check [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- Open an issue in this GitHub repository
