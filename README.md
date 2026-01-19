# YouTube Watchtime Analytics

A complete analytics pipeline for tracking YouTube watch time and engagement metrics, including foreground/background viewing detection.

## Architecture

```
Browser Extension → Flask API → AWS Firehose → S3 → Lambda Processor → S3 (Partials)
                                                                     ↓
                                                    Lambda Compactor → S3 (Aggregated) → Athena → QuickSight
```

## Documentation

- `docs/` - Additional documentation, dashboards, and reports
  - QuickSight dashboard exports
  - Architecture diagrams
  - Additional documentation

## Components

### 1. Browser Extension (`extension/`)
- Chrome extension that tracks YouTube video watching
- Detects foreground/background viewing
- Sends events to local Flask API

### 2. API Server (`api/`)
- Flask server that receives events from extension
- Validates and batches events
- Sends to AWS Firehose

### 3. Lambda Functions (`lambda/`)
- **Processor**: Processes raw events from Firehose, aggregates by channel/video
- **Compactor**: Compacts daily partials into final aggregated data

### 4. Analytics (`athena/`)
- SQL queries for Athena
- Views for daily/weekly analytics
- Ready for QuickSight dashboards

## Setup

### Prerequisites
- Python 3.10+
- AWS account with appropriate permissions
- Chrome browser (for extension)

### 1. API Server Setup

```bash
cd api
cp .env.example .env
# Edit .env with your AWS credentials and Firehose stream name
pip install -r requirements.txt
python server.py
```

See `api/.env.example` for required environment variables.

### 2. Browser Extension Setup

1. Open Chrome and go to `chrome://extensions/`
2. Enable "Developer mode"
3. Click "Load unpacked"
4. Select the `extension/` directory
5. The extension will send events to `http://localhost:4000/ingest`

### 3. Lambda Functions Setup

1. Package each Lambda function:
   ```bash
   # Processor
   cd lambda/processor
   pip install -r requirements.txt -t .
   zip -r processor.zip .

   # Compactor
   cd ../compactor
   pip install -r requirements.txt -t .
   zip -r compactor.zip .
   ```

2. Create Lambda functions in AWS Console
3. Set environment variables (see `lambda/ENVIRONMENT_VARIABLES.md`)
4. Configure triggers:
   - Processor: S3 PutObject event on `raw/` prefix
   - Compactor: Scheduled EventBridge rule (daily)

### 4. AWS Resources

- **S3 Bucket**: Store raw events, partials, and aggregated data
- **Firehose Stream**: Ingest events from API
- **Lambda Functions**: Processor and Compactor
- **Athena**: Query aggregated data
- **QuickSight**: Visualize analytics

### 5. Athena Tables

Run the SQL files in `athena/`:
1. `create_table.sql` - Create external tables
2. `views.sql` - Create views for common queries

## Environment Variables

### API Server (`api/.env`)
- `AWS_REGION` - AWS region
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `FIREHOSE_STREAM_NAME` - Firehose delivery stream name
- `FLASK_HOST` - Flask server host (default: 127.0.0.1)
- `FLASK_PORT` - Flask server port (default: 4000)

### Lambda Functions
See `lambda/ENVIRONMENT_VARIABLES.md` for detailed configuration.

## Data Flow

1. User watches YouTube video → Extension captures events
2. Extension sends events to Flask API (`/ingest`)
3. Flask API batches and sends to Firehose
4. Firehose delivers to S3 (`raw/YYYY/MM/DD/...`)
5. Lambda Processor triggers on S3 PutObject
6. Processor aggregates events → writes partials to S3 (`results/daily/...`)
7. Lambda Compactor (scheduled) aggregates partials → final data (`analytics/...`)
8. Athena queries aggregated data
9. QuickSight visualizes analytics

## Security

- ✅ No secrets in code
- ✅ All secrets in `.env` files (gitignored)
- ✅ Lambda functions use IAM roles (not access keys)
- ✅ `.env` files excluded from git

**Important**: Never commit `.env` files or actual secrets to git!

## Development

```bash
# Start API server
cd api
python server.py

# Test extension
# Load extension in Chrome, watch a YouTube video
# Check API logs and CloudWatch for events
```

## License

[Your License Here]

