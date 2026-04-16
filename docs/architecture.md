# Architecture Deep-Dive

## Solution Architecture

The end-to-end data flow for the workshop:

![Solution Architecture](../diagrams/generated-diagrams/solution-architecture.png)

## Network Architecture

The VPC and infrastructure layout deployed by CloudFormation:

![Network Architecture](../diagrams/generated-diagrams/network-architecture.png)

## Data Flow

1. **Ingestion:** NSE per-minute stock data (5 CSV files, ~2GB total) is synced from a public S3 bucket -> Bastion EC2 -> your workshop S3 bucket
2. **Cataloging:** AWS Glue Crawler auto-discovers the schema and creates a table in the Glue Data Catalog
3. **Preliminary Analysis:** Amazon Athena queries the data directly in S3 using the Glue catalog (serverless, no infrastructure to manage)
4. **Deep Analytics:** Data is imported into EMR HDFS using `s3-dist-cp`, then analyzed with:
   - **Pig Latin** — programmatic data transformations (volume, liquidity, sorting)
   - **Hive SQL** — complex analytics (trading ranges, moving averages, entry/exit points)
   - **Tez/MapReduce** — execution engines (compare performance)
5. **Visualization:** Results are exported to S3 and visualized in Amazon QuickSight

## Network Details

- **VPC** (10.200.0.0/20) isolates all workshop resources
- **Public Subnet** (10.200.0.0/24) — Bastion Host, EMR Master node, NAT Gateway
- **Private Subnet** (10.200.2.0/23) — EMR Core/Task nodes (outbound via NAT)
- **Internet Gateway** — public internet access for the public subnet
- **NAT Gateway** — allows private subnet instances to reach the internet (for package updates, S3 access)

## IAM Roles

| Role | Purpose | Key Permissions |
|------|---------|----------------|
| Bastion Role | EC2 instance profile for Bastion Host | S3 read/write to workshop bucket and source bucket |
| Glue Role | Glue crawler execution | Glue service permissions + S3 access |
| EMR Service Role | EMR cluster management | Standard EMR service role |
| EMR EC2 Role | EMR node instance profile | S3 access + Glue catalog access |

## Security Groups

| Security Group | Ports | Purpose |
|---------------|-------|---------|
| Bastion SG | 22 (SSH) | SSH access from your IP |
| EMR Master SG | 22, 8888, 8088, 8157 | SSH, Hue, Resource Manager, SOCKS proxy |
| EMR Slave SG | (managed by EMR) | Core/Task node communication |

## Cost Optimization Tips

- Use **Spot Instances** for EMR Core/Task nodes (up to 90% savings)
- Terminate the EMR cluster when not actively using it
- The Bastion Host can be stopped between sessions
- Delete the entire CloudFormation stack when done to remove all resources
