# Primis Report Pipeline

Gmail → Unzip → BigQuery · Twice-daily automated ingestion

## Architecture

```
Cloud Scheduler (8am + 6pm UTC)
        │
        ▼ Pub/Sub: primis-scan
Cloud Function (Node.js 20, 2GB, 9min timeout)
        │
        ├── Gmail API → find emails from support@primis.tech
        ├── Download ZIP attachment (200MB+)
        ├── Unzip in memory → raw CSV bytes
        ├── Parse CSV → detect Debug col (B) + Imps col (F)
        ├── Qualify variants: any row with Imps > 10,000
        ├── Stream insert → BigQuery (500-row batches)
        │       ├── primis_reports.media_report
        │       ├── primis_reports.video_report
        │       └── primis_reports.content_report
        └── Pub/Sub: primis-analysis-ready → (optional) trigger analyzer
```

## Files

```
primis-pipeline/
├── cloud-function/
│   ├── index.js          ← main function code
│   └── package.json      ← dependencies
├── bigquery-schema/
│   └── schema.sql        ← table DDL + active_ab_tests view
├── deploy.sh             ← one-shot deployment script
└── README.md
```

## Prerequisites

- Google Cloud project with billing enabled
- `gcloud` CLI installed and authenticated
- Google Workspace admin access (for Gmail domain delegation)
- `garikv@primis.tech` is a Google Workspace account

## Setup

### 1. Clone and configure

```bash
# Edit deploy.sh and replace:
PROJECT_ID="your-actual-project-id"
REGION="us-central1"          # or your preferred region
```

### 2. Run deployment

```bash
chmod +x deploy.sh
./deploy.sh
```

The script will pause and prompt you for the manual Gmail delegation step.

### 3. Gmail Domain-Wide Delegation (manual)

This is required to let the Cloud Function read `garikv@primis.tech` inbox:

1. Go to [Google Admin Console](https://admin.google.com)
2. **Security → API Controls → Manage Domain Wide Delegation**
3. Click **Add new**
4. **Client ID**: get from the service account JSON key file (`client_id` field)
5. **OAuth Scopes**: `https://www.googleapis.com/auth/gmail.readonly`
6. Save

Then in `cloud-function/index.js`, uncomment this line:
```js
client.subject = 'garikv@primis.tech';
```

### 4. Test manually

```bash
gcloud pubsub topics publish primis-scan --message='{"trigger":"manual_test"}'
```

Watch logs:
```bash
gcloud functions logs read primis-report-pipeline --region=us-central1 --limit=50
```

## BigQuery

### Tables

| Table | Partition | Cluster |
|-------|-----------|---------|
| `media_report` | `date` | `debug, site` |
| `video_report` | `date` | `debug, site` |
| `content_report` | `date` | `debug, site` |

### Active A/B test view

The `active_ab_tests` view does the detection automatically:

```sql
SELECT
  report_type,
  test_key,
  ctr_delta_pct,
  rpm_delta_pct,
  cpm_delta_pct,
  has_significant_delta
FROM `YOUR_PROJECT_ID.primis_reports.active_ab_tests`
WHERE has_significant_delta = TRUE
ORDER BY ABS(cpm_delta_pct) DESC;
```

### Detection logic (matches the UI analyzer exactly)

1. A variant qualifies if **any single row** has `imps > 10,000` (not accumulated)
2. Tests are grouped by splitting `debug` on `/` — everything except the last segment
3. Delta is computed as `(active - default) / default * 100`
4. `has_significant_delta = TRUE` if any metric delta exceeds 10%

## Cost estimate

| Component | Estimate |
|-----------|----------|
| Cloud Function (2x daily, ~3min each) | ~$0/month (free tier) |
| BigQuery storage (1 year, ~5GB) | ~$0.10/month |
| BigQuery queries (view scans) | ~$0.05/month |
| Pub/Sub | ~$0/month (free tier) |
| **Total** | **< $1/month** |

## Troubleshooting

**Function times out**: Increase `--timeout` (max 540s for gen2). For very large files consider Cloud Run instead.

**Gmail auth fails**: Verify domain delegation is set up and `client.subject` is uncommented in `index.js`.

**No emails found**: Check `LOOKBACK_DAYS` in config and verify the sender address matches exactly.

**BigQuery insert errors**: Check Cloud Function logs for row-level errors. The function uses `skipInvalidRows: true` so bad rows are skipped, not fatal.
