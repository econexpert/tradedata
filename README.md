# tradedata

Runs in AWS Lambda, uses S3

Event JSON:

{
  "region_of_interest": "428",
  "trade_regime": "2",
  "start_year": "1995",
  "end_year": "2024"
}

Environment variables: 
COMTRADE_API_KEY
COUNTRY_CODES_KEY
S3_BUCKET_NAME
