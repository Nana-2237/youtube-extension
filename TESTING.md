# Testing Guide

## Mock Tests (No AWS Credentials Required)

Run mock tests to verify code logic without AWS:

```cmd
REM Test API Server
cd c:\dev\PythonProject1dd\api
python test_server.py

REM Test Lambda Processor
cd ..\lambda\processor
python test_processor.py

REM Test Lambda Compactor
cd ..\compactor
python test_compactor.py

REM Run all tests
cd c:\dev\PythonProject1dd
python test_all.py
```

## Real Testing (Requires AWS Credentials)

### Required .env File Configuration

Create `api/.env` file with these variables:

```env
# AWS Configuration (REQUIRED)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_actual_access_key_here
AWS_SECRET_ACCESS_KEY=your_actual_secret_key_here

# Firehose Configuration (REQUIRED)
FIREHOSE_STREAM_NAME=your_firehose_stream_name

# Flask Server Configuration (Optional - has defaults)
FLASK_HOST=127.0.0.1
FLASK_PORT=4000
FLASK_DEBUG=True

# Firehose Batching Configuration (Optional - has defaults)
BATCH_MAX_RECORDS=50
BATCH_FLUSH_MS=500
```

### How to Get AWS Credentials

1. **AWS Access Key ID & Secret Access Key:**
   - Go to AWS Console → IAM → Users → Your User → Security Credentials
   - Create Access Key
   - Copy Access Key ID and Secret Access Key
   - **Important**: Store these securely, never commit to git!

2. **Firehose Stream Name:**
   - Go to AWS Console → Kinesis Data Firehose
   - Find your stream name (e.g., `PUT-S3-8JXSX`)
   - Copy the exact name

3. **AWS Region:**
   - Check which region your Firehose stream is in
   - Common: `us-east-1`, `us-west-2`, `eu-west-1`, etc.

### Testing with Real AWS

1. **Create .env file:**
   ```cmd
   cd c:\dev\PythonProject1dd\api
   copy .env.example .env
   ```
   Then edit `.env` with your actual values.

2. **Test API Server:**
   ```cmd
   cd c:\dev\PythonProject1dd\api
   python server.py
   ```

3. **Test Health Endpoint:**
   ```cmd
   curl http://127.0.0.1:4000/health
   ```

4. **Test Ingest Endpoint:**
   ```cmd
   curl -X POST http://127.0.0.1:4000/ingest -H "Content-Type: application/json" -d "{\"events\":[{\"schema\":1,\"event_id\":\"test\",\"event_ts\":1234567890,\"event_type\":\"video_start\",\"client_session_id\":\"test\",\"tab_id\":\"test\",\"video_id\":\"test123\",\"video_session_id\":\"test\"}]}"
   ```

### Lambda Functions Testing

Lambda functions require environment variables set in AWS Console (not .env files):

**For Compactor Lambda:**
- `BUCKET` - Your S3 bucket name
- `PARTIALS_PREFIX` - (optional, default: `results/daily`)
- `OUT_PREFIX` - (optional, default: `analytics`)

**For Processor Lambda:**
- `RAW_PREFIX` - (optional, default: `raw/`)
- `RESULTS_PREFIX` - (optional, default: `results/`)
- `PROCESSED_PREFIX` - (optional, default: `raw-processed/`)

Set these in AWS Console → Lambda → Your Function → Configuration → Environment Variables

### Security Notes

- ✅ `.env` file is gitignored (won't be committed)
- ✅ Never commit actual AWS credentials
- ✅ Use IAM roles for Lambda (not access keys)
- ✅ Rotate access keys regularly

