import boto3
import zipfile
import os

s3 = boto3.client("s3")

MAX_ZIP_SIZE = 500 * 1024 * 1024  # 500 MB per zip (leave buffer under Lambda 512 MB)

def zip_s3_prefix(bucket_name, prefix, output_prefix):
    paginator = s3.get_paginator("list_objects_v2")

    part_num = 1
    tmp_zip = f"/tmp/part{part_num}.zip"
    zf = zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED)

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue

            print(f"Processing {key}...")

            tmp_file = f"/tmp/{os.path.basename(key)}"
            s3.download_file(bucket_name, key, tmp_file)

            # Add file into the zip
            zf.write(tmp_file, arcname=key[len(prefix):])
            os.remove(tmp_file)

            # Check current size of the zip
            current_size = os.path.getsize(tmp_zip)

            # If zip grows too big → upload and start a new one
            if current_size >= MAX_ZIP_SIZE:
                zf.close()
                zip_key = f"{output_prefix}/part{part_num}.zip"
                s3.upload_file(tmp_zip, bucket_name, zip_key)
                print(f"✅ Uploaded {zip_key}")

                # prepare next zip
                part_num += 1
                tmp_zip = f"/tmp/part{part_num}.zip"
                zf = zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED)

    # Close and upload last zip (if it has content)
    zf.close()
    if os.path.exists(tmp_zip) and os.path.getsize(tmp_zip) > 0:
        zip_key = f"{output_prefix}/part{part_num}.zip"
        s3.upload_file(tmp_zip, bucket_name, zip_key)
        print(f"✅ Uploaded {zip_key}")


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET_NAME"]
    results_prefix = os.environ.get("RESULTS_PREFIX", "results/")
    output_prefix = os.environ.get("ZIP_OUTPUT_PREFIX", "archives")

    zip_s3_prefix(bucket, results_prefix, output_prefix)

    return {
        "statusCode": 200,
        "message": f"Zipped files from {results_prefix} into s3://{bucket}/{output_prefix}/part*.zip"
    }
