/**
 * Primis Report Pipeline — Cloud Function
 *
 * Trigger:  Cloud Scheduler → Pub/Sub topic "primis-scan" (twice daily)
 * Flow:     Gmail → find emails from support@primis.tech
 *           → download ZIP attachment
 *           → unzip in memory (streams, no disk)
 *           → parse CSV rows
 *           → stream insert into BigQuery
 *           → publish "analysis-ready" Pub/Sub event
 */

const { google }       = require('googleapis');
const { BigQuery }     = require('@google-cloud/bigquery');
const { PubSub }       = require('@google-cloud/pubsub');
const AdmZip           = require('adm-zip');
const csv              = require('csv-parse/sync');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
  SENDER:          'support@primis.tech',
  BQ_DATASET:      'primis_reports',
  BQ_TABLES: {
    Media:   'media_report',
    Video:   'video_report',
    Content: 'content_report',
  },
  PUBSUB_TOPIC:    'primis-analysis-ready',
  MIN_IMPS_PER_ROW: 10000,   // single-row threshold — NOT accumulated
  DELTA_THRESHOLD:  0.10,    // 10%
  // Column positions (0-based) — auto-detected from header, these are fallbacks
  DEFAULT_DEBUG_COL: 1,      // column B
  DEFAULT_IMPS_COL:  5,      // column F
  // How many days back to look for unprocessed emails
  LOOKBACK_DAYS: 2,
};

const bigquery = new BigQuery();
const pubsub   = new PubSub();

// ── Entry point ───────────────────────────────────────────────────────────────
exports.primisReportPipeline = async (message, context) => {
  console.log('Pipeline triggered at', new Date().toISOString());

  const auth  = await getGmailAuth();
  const gmail = google.gmail({ version: 'v1', auth });

  // 1. Find report emails
  const threads = await findReportEmails(gmail);
  if (!threads.length) {
    console.log('No new Primis report emails found.');
    return;
  }

  console.log(`Found ${threads.length} report thread(s)`);

  const results = [];

  for (const thread of threads) {
    try {
      const result = await processThread(gmail, thread);
      if (result) results.push(result);
    } catch (err) {
      console.error(`Error processing thread ${thread.id}:`, err.message);
    }
  }

  // 2. Publish analysis-ready event with summary
  if (results.length > 0) {
    await publishAnalysisReady(results);
  }

  console.log(`Pipeline complete. Processed ${results.length} report(s).`);
};

// ── Gmail: find report emails ─────────────────────────────────────────────────
async function findReportEmails(gmail) {
  const afterDate = new Date();
  afterDate.setDate(afterDate.getDate() - CONFIG.LOOKBACK_DAYS);
  const afterStr = afterDate.toISOString().split('T')[0].replace(/-/g, '/');

  const query = `from:${CONFIG.SENDER} has:attachment after:${afterStr}`;
  console.log('Gmail query:', query);

  const res = await gmail.users.threads.list({
    userId: 'me',
    q: query,
    maxResults: 10,
  });

  return res.data.threads || [];
}

// ── Process a single email thread ─────────────────────────────────────────────
async function processThread(gmail, thread) {
  const threadData = await gmail.users.threads.get({
    userId: 'me',
    id: thread.id,
    format: 'full',
  });

  for (const message of threadData.data.messages) {
    const parts = message.payload.parts || [];
    for (const part of parts) {
      if (!part.filename || !part.body?.attachmentId) continue;
      if (!part.filename.match(/\.(zip|csv)$/i)) continue;

      console.log(`Processing attachment: ${part.filename}`);

      // Detect report type from filename or subject
      const reportType = detectReportType(part.filename, message.payload.headers);
      if (!reportType) {
        console.log(`Could not detect report type for ${part.filename}, skipping`);
        continue;
      }

      // Download attachment
      const attachmentData = await gmail.users.messages.attachments.get({
        userId: 'me',
        messageId: message.id,
        id: part.body.attachmentId,
      });

      const buffer = Buffer.from(attachmentData.data.data, 'base64');
      console.log(`Downloaded ${part.filename} — ${(buffer.length / 1e6).toFixed(1)} MB`);

      // Unzip if needed
      let csvBuffer;
      if (part.filename.toLowerCase().endsWith('.zip')) {
        csvBuffer = unzipReport(buffer, part.filename);
      } else {
        csvBuffer = buffer;
      }

      if (!csvBuffer) {
        console.error(`Failed to extract CSV from ${part.filename}`);
        continue;
      }

      console.log(`CSV size: ${(csvBuffer.length / 1e6).toFixed(1)} MB`);

      // Parse + load into BigQuery
      const stats = await parseAndLoad(csvBuffer, reportType);

      return {
        filename: part.filename,
        reportType,
        messageId: message.id,
        ...stats,
      };
    }
  }

  return null;
}

// ── Detect report type from filename/subject ──────────────────────────────────
function detectReportType(filename, headers) {
  const subject = (headers?.find(h => h.name === 'Subject')?.value || '').toLowerCase();
  const name    = filename.toLowerCase();
  const text    = name + ' ' + subject;

  if (text.includes('media'))   return 'Media';
  if (text.includes('video'))   return 'Video';
  if (text.includes('content')) return 'Content';
  return null;
}

// ── Unzip: extract first CSV found ───────────────────────────────────────────
function unzipReport(buffer) {
  const zip = new AdmZip(buffer);
  const entries = zip.getEntries();

  for (const entry of entries) {
    if (entry.entryName.match(/\.csv$/i) && !entry.isDirectory) {
      console.log(`Extracting: ${entry.entryName} (${(entry.header.size / 1e6).toFixed(1)} MB uncompressed)`);
      return entry.getData(); // returns Buffer
    }
  }

  console.error('No CSV found inside ZIP');
  return null;
}

// ── Parse CSV + stream to BigQuery ────────────────────────────────────────────
async function parseAndLoad(csvBuffer, reportType) {
  const tableName = CONFIG.BQ_TABLES[reportType];
  const table     = bigquery.dataset(CONFIG.BQ_DATASET).table(tableName);

  const text = csvBuffer.toString('utf8');

  // Find header row (scan first 15 lines)
  const lines   = text.split(/\r?\n/);
  let headerIdx = -1;
  for (let i = 0; i < Math.min(lines.length, 15); i++) {
    const lower = lines[i].toLowerCase();
    if (lower.includes('debug') && lower.includes('imp')) {
      headerIdx = i;
      break;
    }
  }

  // Parse header to find column positions
  const headerLine = headerIdx >= 0 ? lines[headerIdx] : null;
  const colMap     = detectColumns(headerLine);
  console.log('Column map:', colMap);

  // Parse CSV from detected header row
  const dataLines = lines.slice(headerIdx >= 0 ? headerIdx : 0).join('\n');
  const records   = csv.parse(dataLines, {
    columns:          true,
    skip_empty_lines: true,
    trim:             true,
    relax_column_count: true,
  });

  console.log(`Parsed ${records.length.toLocaleString()} rows`);

  // ── KEY LOGIC: qualify variants by single-row impression threshold ──────────
  const variantQualified = new Set();

  for (const row of records) {
    const debug = getCol(row, colMap.debug, 'debug');
    const imps  = parseNum(getCol(row, colMap.imps, 'imps.'));
    if (debug && imps > CONFIG.MIN_IMPS_PER_ROW) {
      variantQualified.add(debug);
    }
  }

  console.log(`Variants with at least one row > ${CONFIG.MIN_IMPS_PER_ROW.toLocaleString()} imps: ${variantQualified.size}`);

  // ── Stream qualified rows to BigQuery in batches of 500 ────────────────────
  const BATCH_SIZE  = 500;
  let   inserted    = 0;
  let   skipped     = 0;
  let   batch       = [];

  const ingestDate = new Date().toISOString();

  for (const row of records) {
    const debug = getCol(row, colMap.debug, 'debug');
    if (!variantQualified.has(debug)) { skipped++; continue; }

    const bqRow = buildBQRow(row, colMap, reportType, ingestDate);
    batch.push(bqRow);

    if (batch.length >= BATCH_SIZE) {
      await table.insert(batch, { skipInvalidRows: true, ignoreUnknownValues: true });
      inserted += batch.length;
      batch = [];
      if (inserted % 10000 === 0) console.log(`Inserted ${inserted.toLocaleString()} rows...`);
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    await table.insert(batch, { skipInvalidRows: true, ignoreUnknownValues: true });
    inserted += batch.length;
  }

  console.log(`BigQuery insert complete: ${inserted.toLocaleString()} rows inserted, ${skipped.toLocaleString()} skipped`);

  return {
    totalRows:        records.length,
    insertedRows:     inserted,
    skippedRows:      skipped,
    qualifiedVariants: [...variantQualified],
  };
}

// ── Build a BigQuery row object ───────────────────────────────────────────────
function buildBQRow(row, colMap, reportType, ingestDate) {
  return {
    ingest_timestamp: ingestDate,
    report_type:      reportType,
    date:             getCol(row, colMap.date,   'date')   || null,
    debug:            getCol(row, colMap.debug,  'debug')  || null,
    site:             getCol(row, colMap.site,   'site')   || null,
    size:             getCol(row, colMap.size,   'size')   || null,
    format:           getCol(row, colMap.format, 'format') || null,
    imps:             parseNum(getCol(row, colMap.imps,    'imps.'))   || 0,
    clicks:           parseNum(getCol(row, colMap.clicks,  'clicks'))  || 0,
    ctr:              parseNum(getCol(row, colMap.ctr,     'ctr'))     || 0,
    rpm:              parseNum(getCol(row, colMap.rpm,     'rpm'))     || 0,
    cpm:              parseNum(getCol(row, colMap.cpm,     'cpm'))     || 0,
    revenue:          parseNum(getCol(row, colMap.revenue, 'revenue')) || 0,
  };
}

// ── Column detection helpers ──────────────────────────────────────────────────
function detectColumns(headerLine) {
  if (!headerLine) {
    return {
      debug: null, imps: null, date: null,
      site: null,  size: null, format: null,
      clicks: null, ctr: null, rpm: null, cpm: null, revenue: null,
    };
  }

  const cols = headerLine.split(',').map((c, i) => ({ name: c.trim().toLowerCase(), i }));
  const find = (...terms) => (cols.find(c => terms.some(t => c.name.includes(t))) || {}).name || null;

  return {
    debug:   find('debug'),
    imps:    find('imps'),
    date:    find('date'),
    site:    find('site', 'domain'),
    size:    find('size', 'dimension'),
    format:  find('format', 'type'),
    clicks:  find('click'),
    ctr:     find('ctr'),
    rpm:     find('rpm', 'ecpm'),
    cpm:     find('cpm'),
    revenue: find('rev', 'earn', 'income'),
  };
}

function getCol(row, colName, fallback) {
  if (colName && row[colName] !== undefined) return row[colName];
  // Try fallback fuzzy match
  const key = Object.keys(row).find(k => k.toLowerCase().includes(fallback));
  return key ? row[key] : '';
}

function parseNum(val) {
  if (!val) return 0;
  return parseFloat(val.toString().replace(/[,$%\s]/g, '')) || 0;
}

// ── Publish analysis-ready Pub/Sub message ────────────────────────────────────
async function publishAnalysisReady(results) {
  const topic   = pubsub.topic(CONFIG.PUBSUB_TOPIC);
  const payload = Buffer.from(JSON.stringify({ results, timestamp: new Date().toISOString() }));
  await topic.publish(payload);
  console.log('Published analysis-ready event to Pub/Sub');
}

// ── Gmail OAuth2 auth (uses Cloud Function service account + domain delegation)
async function getGmailAuth() {
  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/gmail.readonly',
    ],
  });
  const client = await auth.getClient();
  // If using domain-wide delegation, impersonate the inbox owner:
  // client.subject = 'garikv@primis.tech';
  return client;
}
