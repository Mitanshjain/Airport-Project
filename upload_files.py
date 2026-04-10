import boto3
from botocore.exceptions import ClientError
import os


def upload_files_to_s3(bucket_name, folder_path, region):

    s3_client = boto3.client('s3', region_name=region)

    all_files = os.listdir(folder_path)

    if len(all_files) == 0:
        print("No files found in the folder!")
        return

    print(f"Total files found: {len(all_files)}")
    print(f"Uploading to bucket: '{bucket_name}'")
    print("=" * 60)

    success_count = 0
    failed_count  = 0

    for file_name in all_files:

        local_file_path = os.path.join(folder_path, file_name)

        if not os.path.isfile(local_file_path):
            continue

        try:
            print(f"Uploading: {file_name} ...")

            # EXTRACT YEAR AND MONTH FROM FILE NAME
            parts = file_name.split("_")
            year = parts[-2]
            month = parts[-1].replace(".csv", "").zfill(2)

           
            s3_key = f"flight-data/{year}/Month{month}/{file_name}"

            # upload file
            s3_client.upload_file(local_file_path, bucket_name, s3_key)

            print(f"Uploaded successfully: {s3_key} ")
            success_count += 1

        except ClientError as e:
            print(f"Failed to upload: {file_name} ")
            print(f"Error: {e}")
            failed_count += 1

    print("\n" + "=" * 60)
    print("UPLOAD SUMMARY")
    print("=" * 60)
    print(f"Total files    : {len(all_files)}")
    print(f"Uploaded       : {success_count} ")
    print(f"Failed         : {failed_count} ")
    print("=" * 60)


# -------- MAIN EXECUTION --------
if __name__ == "__main__":

    bucket_name = input("Enter your bucket name: ")
    region = input("Enter your region (example: us-east-1): ")

    folder_path = "downloaded_files"

    upload_files_to_s3(bucket_name, folder_path, region)