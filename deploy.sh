#!/bin/bash
# =============================================================
#  Primis Pipeline — Full Deployment Script
#  Run this once from your local machine (gcloud CLI required)
#  Replace all YOUR_* placeholders before running
# =============================================================

set -e  # exit on any error

# ── Variables — edit these ─────────────────────────────────────
PROJECT_ID="YOUR_PROJECT_ID"
REGION="us-central1"
GMAIL_USER="garikv@primis.tech"
FUNCTION_NAME="primis-report-pipeline"
SA_NAME="primis-pipeline-sa"
PUBSUB_TRIGGER_TOPIC="primis-scan"
PUBSUB_OUTPUT_TOPIC="primis-analysis-ready"
SCHEDULER_NAME="primis-twice-daily"
BQ_DATASET="primis_reports"

echo "🚀 Starting Primis Pipeline deployment for project: $PROJECT_ID"

# ── 0. Set project ─────────────────────────────────────────────
gcloud config set project $PROJECT_ID

# ── 1. Enable required APIs ────────────────────────────────────
echo "Enabling APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  gmail.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com

# ── 2. Create service account ──────────────────────────────────
echo "Creating service account..."
gcloud iam service-accounts create $SA_NAME \
  --display-name="Primis Pipeline Service Account" \
  --description="Used by the Gmail→BigQuery pipeline Cloud Function" \
  || echo "Service account already exists"

SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# ── 3. Grant IAM roles to service account ─────────────────────
echo "Granting IAM roles..."

# BigQuery: insert rows
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

# BigQuery: run jobs
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

# Pub/Sub: publish messages
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/pubsub.publisher"

# Cloud Functions: invoker (for scheduler)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/cloudfunctions.invoker"

# ── 4. Gmail Domain-Wide Delegation ───────────────────────────
echo ""
echo "⚠️  MANUAL STEP REQUIRED: Gmail Domain-Wide Delegation"
echo "──────────────────────────────────────────────────────"
echo "1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts"
echo "2. Click service account: $SA_EMAIL"
echo "3. Click 'Keys' → 'Add Key' → 'Create new key' → JSON → Download"
echo "4. Go to Google Workspace Admin: https://admin.google.com"
echo "5. Security → API Controls → Domain-wide delegation → Add new"
echo "6. Client ID: (find in the downloaded JSON as 'client_id')"
echo "7. OAuth scopes: https://www.googleapis.com/auth/gmail.readonly"
echo "8. Save the downloaded JSON as 'service-account-key.json' in cloud-function/"
echo ""
read -p "Press Enter once you've completed the Gmail delegation step..."

# ── 5. Create Pub/Sub topics ───────────────────────────────────
echo "Creating Pub/Sub topics..."
gcloud pubsub topics create $PUBSUB_TRIGGER_TOPIC  || echo "Topic already exists"
gcloud pubsub topics create $PUBSUB_OUTPUT_TOPIC   || echo "Topic already exists"

# ── 6. Create BigQuery dataset and tables ─────────────────────
echo "Creating BigQuery dataset and tables..."
bq --project_id=$PROJECT_ID mk \
  --dataset \
  --location=US \
  --description="Primis report data ingested from Gmail" \
  $BQ_DATASET \
  || echo "Dataset already exists"

# Run schema SQL (requires bq CLI)
bq --project_id=$PROJECT_ID query \
  --use_legacy_sql=false \
  --nouse_cache \
  "$(sed "s/YOUR_PROJECT_ID/$PROJECT_ID/g" ../bigquery-schema/schema.sql)"

echo "BigQuery tables created"

# ── 7. Deploy Cloud Function ───────────────────────────────────
echo "Deploying Cloud Function..."
cd cloud-function

gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=nodejs20 \
  --region=$REGION \
  --source=. \
  --entry-point=primisReportPipeline \
  --trigger-topic=$PUBSUB_TRIGGER_TOPIC \
  --service-account=$SA_EMAIL \
  --memory=2GB \
  --timeout=540s \
  --set-env-vars="GMAIL_USER=$GMAIL_USER,GCP_PROJECT=$PROJECT_ID" \
  --min-instances=0 \
  --max-instances=3

cd ..

echo "Cloud Function deployed"

# ── 8. Create Cloud Scheduler jobs (twice daily) ───────────────
echo "Creating Cloud Scheduler jobs..."

# Morning scan: 8:00 AM UTC
gcloud scheduler jobs create pubsub ${SCHEDULER_NAME}-morning \
  --schedule="0 8 * * *" \
  --topic=$PUBSUB_TRIGGER_TOPIC \
  --message-body='{"trigger":"morning_scan"}' \
  --location=$REGION \
  --description="Primis report morning scan" \
  --time-zone="UTC" \
  || echo "Morning scheduler already exists"

# Evening scan: 6:00 PM UTC
gcloud scheduler jobs create pubsub ${SCHEDULER_NAME}-evening \
  --schedule="0 18 * * *" \
  --topic=$PUBSUB_TRIGGER_TOPIC \
  --message-body='{"trigger":"evening_scan"}' \
  --location=$REGION \
  --description="Primis report evening scan" \
  --time-zone="UTC" \
  || echo "Evening scheduler already exists"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "Summary:"
echo "  Cloud Function : $FUNCTION_NAME ($REGION)"
echo "  Trigger topic  : $PUBSUB_TRIGGER_TOPIC"
echo "  Output topic   : $PUBSUB_OUTPUT_TOPIC"
echo "  BigQuery       : $PROJECT_ID.$BQ_DATASET"
echo "  Schedules      : 08:00 UTC + 18:00 UTC daily"
echo ""
echo "To trigger manually:"
echo "  gcloud pubsub topics publish $PUBSUB_TRIGGER_TOPIC --message='{\"trigger\":\"manual\"}'"
echo ""
echo "To query active A/B tests in BigQuery:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM \`$PROJECT_ID.$BQ_DATASET.active_ab_tests\`'"
