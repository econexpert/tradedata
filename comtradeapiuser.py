import urllib.request
import urllib.error
import time
import json
import boto3
import os

MAX_RUNTIME = 750  # seconds
TMP_PROGRESS_FILE = "/tmp/progress.json"

countries = [4, 8, 12, 20, 24, 28, 31, 32, 36, 40, 44, 48, 50, 51, 52, 56, 60, 64, 68, 70, 72, 76, 84, 96, 100, 104,
             108, 112, 116, 120, 124, 136, 140, 144, 148, 152, 156, 170, 178, 188, 191, 192, 196, 203, 204, 208, 212,
             214, 218, 222, 226, 231, 232, 233, 242, 246, 251, 262, 266, 268, 270, 276, 288, 300, 308, 312, 320, 324,
             328, 332, 340, 344, 348, 352, 360, 364, 368, 372, 376, 381, 384, 388, 392, 398, 400, 404, 408, 410, 414,
             417, 418, 422, 426, 428, 430, 434, 440, 442, 446, 450, 454, 458, 462, 466, 470, 478, 480, 484, 490, 496,
             498, 499, 504, 508, 512, 516, 524, 528, 554, 558, 562, 566, 579, 586, 591, 598, 600, 604, 608, 616, 620,
             624, 626, 634, 642, 643, 646, 682, 686, 688, 690, 694, 699, 702, 703, 704, 705, 706, 710, 716, 724, 740,
             748, 752, 757, 760, 762, 764, 768, 776, 780, 784, 788, 792, 795, 800, 804, 818, 826, 834, 842, 854, 858,
             860, 862, 894, 899]


def load_country_dict(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    json_data = json.loads(obj["Body"].read().decode("utf-8"))
    return {int(item['id']): item['text'] for item in json_data['results'] if item['id'].isdigit()}


def load_progress(bucket, key):
    # First check local temp file
    if os.path.exists(TMP_PROGRESS_FILE):
        with open(TMP_PROGRESS_FILE, "r") as f:
            return json.load(f)

    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        progress = json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        progress = {}

    # Write it to tmp for local updates
    with open(TMP_PROGRESS_FILE, "w") as f:
        json.dump(progress, f)

    return progress


def save_progress_local(progress):
    with open(TMP_PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def flush_progress_to_s3(bucket, key):
    if os.path.exists(TMP_PROGRESS_FILE):
        with open(TMP_PROGRESS_FILE, "r") as f:
            data = f.read()
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)


def load_existing_results(bucket, key):
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []


def save_results(bucket, key, records):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(records).encode("utf-8"))


def lambda_handler(event, context):
    start_time = time.time()

    bucket_name = os.environ["S3_BUCKET_NAME"]
    json_key = os.environ["COUNTRY_CODES_KEY"]
    subkey = os.environ["COMTRADE_API_KEY"]

    region = int(event["region_of_interest"])
    regime = int(event["trade_regime"])
    start_year = int(event["start_year"])
    end_year = int(event["end_year"])

    country_dict = load_country_dict(bucket_name, json_key)
    country_name = country_dict.get(region, "Unknown_Country")
    trade_type = "imports" if regime == 1 else "exports"

    result_key = f"results/{country_name.lower().replace(' ', '_')}_{trade_type}_{start_year}to{end_year}.json"
    progress_key = f"progress/{country_name.lower().replace(' ', '_')}_{trade_type}_{start_year}to{end_year}_progress.json"

    progress = load_progress(bucket_name, progress_key)
    last_position = progress.get("last_position", {})
    sleep_time = 1

    existing_records = load_existing_results(bucket_name, result_key)
    new_records = []

    print(f"Resuming download: {country_name} ({trade_type}, {start_year}-{end_year})")

    for year in range(start_year, end_year + 1):
        if str(year) not in progress:
            progress[str(year)] = []

        for partner_code in countries:
            if partner_code == region:
                continue

            if last_position:
                target_year = last_position.get("year", start_year)
                target_partner = last_position.get("partner_code", 0)

                if year < target_year:
                    if len(progress.get(str(year), [])) >= len(countries) - 1:
                        continue
                elif year == target_year and partner_code < target_partner:
                    continue

                last_position = {}

            if partner_code in progress[str(year)]:
                continue

            url = (f"https://comtradeapi.un.org/data/v1/get/C/A/HS?cmdCode=all&period={year}"
                   f"&reporterCode={region}&partnerCode={partner_code}"
                   f"&flowCode={'M' if regime == 1 else 'X'}"
                   f"&maxrecords=500&subscription-key={subkey}")

            while True:
                if time.time() - start_time >= MAX_RUNTIME:
                    print("⏳ Time limit hit. Saving results and progress to S3.")
                    existing_records.extend(new_records)
                    save_results(bucket_name, result_key, existing_records)

                    progress["last_position"] = {"year": year, "partner_code": partner_code}
                    save_progress_local(progress)
                    flush_progress_to_s3(bucket_name, progress_key)

                    return {
                        "statusCode": 206,
                        "message": "Partial run complete. Re-invoke to continue.",
                        "last_year": year,
                        "last_partner_code": partner_code
                    }

                try:
                    response = urllib.request.urlopen(url, timeout=15)
                    raw = response.read().decode("utf-8")
                    response.close()
                    parsed = json.loads(raw)
                    records = parsed.get("data", [])

                    if records:
                        new_records.extend(records)
                        print(f"✅ {year}/{partner_code}: {len(records)} records collected")
                    else:
                        print(f"⚠️ {year}/{partner_code}: No data")

                    progress[str(year)].append(partner_code)
                    progress["last_position"] = {"year": year, "partner_code": partner_code}
                    save_progress_local(progress)

                    sleep_time = max(1, sleep_time - 1)
                    break

                except urllib.error.HTTPError as e:
                    if e.code == 403:
                        print(f"🚨 HTTP 403 Quota Exceeded for {year}/{partner_code}. Exiting early.")
                        # Save results and progress immediately
                        existing_records.extend(new_records)
                        save_results(bucket_name, result_key, existing_records)
                        save_progress_local(progress)
                        flush_progress_to_s3(bucket_name, progress_key)
                        return {
                            "statusCode": 403,
                            "message": f"Quota exceeded at {year}/{partner_code}. Partial results saved.",
                            "last_year": year,
                            "last_partner_code": partner_code
                        }
                    else:
                        print(f"❌ HTTP error for {year}/{partner_code}: {e}")
                        sleep_time += 1
                        time.sleep(sleep_time)

                except Exception as e:
                    print(f"❌ Unexpected error for {year}/{partner_code}: {e}")
                    sleep_time += 1
                    time.sleep(sleep_time)

    existing_records.extend(new_records)
    save_results(bucket_name, result_key, existing_records)

    progress["last_position"] = {}
    save_progress_local(progress)
    flush_progress_to_s3(bucket_name, progress_key)

    print("🎉 All data fetched and saved.")

    return {
        "statusCode": 200,
        "message": "Download complete.",
        "output_file": f"s3://{bucket_name}/{result_key}"
    }
