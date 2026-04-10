# check_all_buckets.py
# Purpose: Connect to AWS S3 and list / check all buckets in your account

import boto3  # AWS for Python - used to interact with AWS services

def list_all_buckets():
    """
    Connects to AWS S3 and prints all the bucket names
    available in the configured AWS account.
    """

    # Create an S3 client using boto3 (uses credentials from ~/.aws/credentials)
    s3_client = boto3.client('s3')

    # Call the S3 API to get all buckets in the account
    response = s3_client.list_buckets()

    # Extract the list of buckets from the response dictionary
    buckets = response.get('Buckets', [])  # Returns empty list if no buckets found

    # Check if any buckets exist
    if buckets:
        print(f"Total buckets found: {len(buckets)}")  # Print total count
        print("-" * 40)  # Divider line for readability

        # Loop through each bucket and print its name and creation date
        for bucket in buckets:
            print(f"Bucket Name : {bucket['Name']}")           # Bucket name
            print(f"Created On  : {bucket['CreationDate']}")   # When it was created
            print("-" * 40)  
    else:
        print("No buckets found in your AWS account.")  # No buckets exist


# -------- MAIN EXECUTION --------
if __name__ == "__main__":
    list_all_buckets()  # Run the function when script is executed directly