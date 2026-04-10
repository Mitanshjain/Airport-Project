# create_bucket.py
# Purpose: User enters bucket name and region, code creates the bucket on S3

import boto3                                 # to connect to AWS S3
from botocore.exceptions import ClientError  # to catch AWS errors


def create_bucket(bucket_name, region):
    
    # Creates a new bucket on AWS S3.

    # Parameters:
    #     bucket_name  : name of the bucket to create
    #     region       : AWS region where bucket will be created


    # create connection to S3
    s3_client = boto3.client('s3', region_name=region)

    try:
        
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)

        else:
            # all other regions need LocationConstraint
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region
                }
            )

        print(f"Bucket '{bucket_name}' created successfully in '{region}'!")

    except ClientError as e:
        print(f"Failed to create bucket. Error: {e}")


# -------- MAIN EXECUTION --------
if __name__ == "__main__":

    # ask user to enter bucket name
    bucket_name = input("Enter bucket name to create: ")

    # ask user to enter region
    print("\nAvailable regions:")
    print("  1. us-east-1      (N. Virginia)")
    print("  2. us-west-2      (Oregon)")
    print("  3. ap-south-1     (Mumbai)")
    print("  4. eu-west-1      (Ireland)")

    # user types region
    region = input("\nEnter your region: ")

    # call the function
    create_bucket(bucket_name, region)