import boto3
import toml
import os

def read_sam_config(path="samconfig.toml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo {path} não encontrado.")
    
    config = toml.load(path)
    region = config['default']['deploy']['parameters']['region']
    stack_name = config['default']['deploy']['parameters']['stack_name']
    return region, stack_name

def setup_clients(region):
    cf = boto3.client('cloudformation', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    s3_client = boto3.client('s3', region_name=region)
    return cf, lambda_client, s3_client

def get_stack_outputs(cf, stack_name):
    print(f"[+] Searching outputs for stack: {stack_name}")
    try:
        response = cf.describe_stacks(StackName=stack_name)
        outputs = response['Stacks'][0]['Outputs']
    except Exception as e:
        print(f"[-] Error fetching outputs:...")
        return {}

    return {o['ExportName']: o['OutputValue'] for o in outputs}

def add_lambda_permission(lambda_client, function_name, bucket_arn):
    try:
        print(f"\n[+] Adding permission for bucket to invoke function {function_name}")
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId="s3invoke-processbronze",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=bucket_arn,
        )
        print("[✔] Permission added.")
    except lambda_client.exceptions.ResourceConflictException:
        print("[!] Permission already exists. Skipping...")

def configure_s3_notification(s3_client, bucket_name, lambda_arn):
    print(f"[+] Configuring notification trigger on bucket: {bucket_name}")
    notification_config = {
        "LambdaFunctionConfigurations": [
            {
                "Id": "BronzeToSilverTrigger",
                "LambdaFunctionArn": lambda_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "bronze/"},
                            {"Name": "suffix", "Value": "_SUCCESS"},
                        ]
                    }
                }
            }
        ]
    }

    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_config
        )
        print("[✔] Trigger configured successfully.")
    except Exception as e:
        print(f"[-] Error configuring trigger: {e}")

if __name__ == "__main__":
    region, stack_name = read_sam_config()
    cf, lambda_client, s3_client = setup_clients(region)

    outputs = get_stack_outputs(cf, stack_name)

    bronze_bucket_name = outputs["IMDB-BronzeBucketName"]
    bronze_bucket_arn = f"arn:aws:s3:::{bronze_bucket_name}"
    lambda_function_arn = outputs["IMDB-ProcessBronzeToSilverFunctionArn"]
    lambda_function_name = lambda_function_arn.split(":")[-1]

    add_lambda_permission(lambda_client, lambda_function_name, bronze_bucket_arn)
    configure_s3_notification(s3_client, bronze_bucket_name, lambda_function_arn)

    print("[✔] Configuration completed successfully!")