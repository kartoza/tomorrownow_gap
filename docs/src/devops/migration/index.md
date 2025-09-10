# Migration from MinIO to DigitalOcean Spaces

## Data Migration (Effort and Time)

### Scope & Key Questions

**Scope Definition**:

  - Is the migration limited to the **TNGAP products** bucket?
  - Are we dealing exclusively with **static files**, or do any of the files undergo frequent updates requiring minimal disruption?

**Timing**:

  - When can the migration be executed to **avoid conflicts with active CRUD operations**?
  
**Metadata & Versioning**:

  - Is it **necessary to retain version history** or metadata (e.g., tags, last-modified timestamps)?
  - This is a critical consideration, as tools like `mc mirror` **do not preserve versioning or metadata**.

### Approach Options

**Option 1: `mc mirror` (Recommended for simplicity and efficiency)**

  - **Advantages**:
    - Fast and suitable for one-way mirroring.
    - Simple to script and automate.
    - Effective across S3-compatible endpoints with differing credentials.
  - **Limitations**:
    - Does not preserve version history or bucket-level metadata (e.g., object tags, lifecycle policies).

#### Example script

A sample Bash script to automate the migration using `mc mirror`:

```bash linenums="1"
--8<-- "src/devops/migration/mc_mirror_demo.sh"
```

**Option 2: Custom scripting using `aws s3 sync` (via AWS S3 compatibility layer)**

  - Offers more granular control if needed.
  - But DigitalOcean Spaces' AWS compatibility is partial—**not all SDK features work reliably**.

### Time & Effort Estimate
The following estimates are based on the assumption that the only target for migration is the `tngap-products` bucket (approximately 85–125 GiB as of 18-04-2024):

**Average Transfer Speed**: Estimated at 30–50 MiB/s (dependent on network conditions).

**Migration Duration**:

  - `85 GiB / 40 MiB/s ≈ 2–2.5 hours`
  - `125 GiB / 40 MiB/s ≈ 3–3.5 hours`

**Estimated Activity Breakdown**:

| Activity                                        | Time Estimate     |
|------------------------------------------------|-------------------|
| Creating bucket policies                       | 1.5 hours         |
| Migrating users/groups to DO Teams             | 1 hour            |
| Dry-run sync                                   | 15–30 minutes     |
| Initial data sync                              | 2–3.5 hours       |
| Final delta sync                               | 30 minutes–1 hour |
| Validating post-migration access rules         | 1 hour            |
| Reconfiguring TNGAP application endpoints      | 1–2 hours         |
| Post-migration monitoring                      | 1–2 hours         |

---

## DigitalOcean Costing

### Cost Components

**Storage**:

  - $0.02 per GiB per month.
  - The first **250 GiB** is included in the **$5/month** Spaces subscription (across all buckets).
    - This threshold has already been exceeded.
  - Additional usage is billed at $0.02/GiB/month.

**Data Transfer**:

  - **Inbound (upload)**: Free
  - **Outbound (download)**:
    - The first **1 TB/month** is included in the subscription (across all buckets).
    - Usage exceeding this is billed at **$0.01/GiB per month**.

**CDN (Optional)**:

  - CDN can be enabled per bucket at **no additional cost**.
  - Offers performance improvements for distributed clients and may help reduce backend bandwidth costs.

---

## User/System Access to DO Spaces

### Access Control Options

#### DigitalOcean Console Access

**Question**: Should TNGAP users be granted access to the DigitalOcean web console?

  - If **yes**, users must be added to the appropriate DO Team by an administrator.
  - Users with sufficient permissions can generate their own access keys if needed.
- Note that **fine-grained IAM is not supported**. DigitalOcean Teams only offer **predefined roles**:
  > [DigitalOcean Teams currently do not support custom team roles. They offer a set of six predefined roles: Owner, Biller, Billing Viewer, Member, Modifier, and Resource Viewer.](https://docs.digitalocean.com/platform/teams/roles/predefined/)

#### Programmatic Access

Managed through:

  - **Access keys** (Access Key ID and Secret Access Key).
  - **Bucket-level policies** configured via Terraform or the DO API.

There is **no equivalent IAM system** as seen in Minio. Access key lifecycle must be handled manually or via external automation.

#### Migration of IAM Users from MinIO

- DigitalOcean does not natively support MinIO-style IAM user/group constructs.

**Recommended Approach**:

  - Identify key service accounts and human users.
  - Provision **separate access keys** for each service or integration.
  - Use `digitalocean_spaces_bucket_policy` to apply principle of least privilege where applicable.

### References

- [Terraform: `digitalocean_spaces_bucket_policy`](https://registry.terraform.io/providers/digitalocean/digitalocean/latest/docs/resources/spaces_bucket_policy)  
- [DigitalOcean Spaces Pricing](https://docs.digitalocean.com/products/spaces/details/pricing/)
