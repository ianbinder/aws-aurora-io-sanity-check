# aws-aurora-io-sanity-check

As of this writing, AWS states you should choose IO Optimized configurations for your Aurora Postgres Database when:
"I/O costs >25% of total database costs"

Testing has shown that this does not always work out, and you can only change this option once every 30 days, so you may be in for a bit of a suprise when you receive your bill. This is mostly due to the increase in storage costs. 

This function will use your last months IO and storage costs to determine if IO optimized will be a good fit for you. This is a function built to be run as a Lambda. The role it runs as has permissions to assume a role named "readonly" in all child accounts from the payer. The "readonly" account has read only access to RDS and Cloudwatch.
