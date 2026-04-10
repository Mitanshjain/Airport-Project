# bucket_exists.py
# Purpose: User enters bucket name and code checks if it exists on S3 or not

import boto3                                 # to connect to AWS S3
from botocore.exceptions import ClientError  # to catch AWS errors


def does_bucket_exist(bucket_name):
    """
    Checks if the given bucket exists on S3 or not.

    Parameter:
        bucket_name (str) : name of the bucket to check
    """

    # create connection to S3
    s3_client = boto3.client('s3')

    try:
        # check if bucket exists on S3
        s3_client.head_bucket(Bucket=bucket_name)
        return True   # bucket found

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            return False  # bucket not found
        else:
            print(f"Error: {e}")
            return False


# -------- MAIN EXECUTION --------
if __name__ == "__main__":

    # ask user to enter bucket name
    bucket_name = input("Enter bucket name to check: ")

    # call the function
    result = does_bucket_exist(bucket_name)

    # print result
    if result:
        print(f"Bucket '{bucket_name}' EXISTS on S3")
    else:
        print(f"Bucket '{bucket_name}' does NOT exist on S3")