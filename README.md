CertSight
=========
CertSight is a scalable system for faster querying of Certificate Transparency (CT) logs, using a TLD-based database schema to improve performance and simplify domain-specific analyses.

## Main Idea:
The inspiration for our new system, CertSight, was drawn from the hierarchical and distributed design of the Domain Name Sys- tem (DNS). In DNS, each Top-Level Domain (TLD) has its own resolver, enabling scalability and efficient management of domain name queries. CertSight mirrors this architecture by creating a dedicated database for each TLD, ensuring logical isolation of data and improving query performance. This approach allows CertSight to handle the vast and growing dataset of Certificate Transparency logs efficiently. Within each TLD-specific database, CertSight organizes entries into tables based on Common Names (CNs) using regex-based mappings, similar to how DNS organizes subdomains under their parent domains. This hierarchical design ensures scalability while optimizing performance for domain-specific queries. By drawing inspiration from the DNS system, CertSight combines a proven, scalable approach with domain-specific flexibility to effectively manage and analyze Certificate Transparency log data.

Read the report certsight.pdf to know more about the detailed architecture and the problem statement solved using CertSight.

Data present in PACE scorrea34/scratch/CS-8803-SII.
