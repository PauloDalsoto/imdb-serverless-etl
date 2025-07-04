import boto3
import toml
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_sam_config(path="samconfig.toml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo {path} não encontrado.")
    
    config = toml.load(path)
    region = config['default']['deploy']['parameters']['region']
    buckets = get_bucket_names_from_toml(config)

    return region, buckets

def get_bucket_names_from_toml(config):
    params = config.get('default', {}).get('deploy', {}).get('parameters', {})
    parameter_overrides_list = params.get('parameter_overrides', [])
    bucket_params = {}

    for override in parameter_overrides_list:
        if '=' in override:
            key, value = override.split('=', 1) 
            bucket_params[key] = value.strip() 
    return bucket_params

def empty_s3_buckets(buckets, region_name):
    s3_resource = boto3.resource('s3', region_name=region_name)

    for bucket_name in buckets.values():
        logging.info(f"Startiing clean up for: {bucket_name} bucket.")
        bucket = s3_resource.Bucket(bucket_name)

        try:
            bucket.objects.all().delete()
            logging.info(f"All objects deleted from '{bucket_name}' bucket.")

        except Exception as e:
            logging.error(f"Error while deleting bucket '{bucket_name}': {e}")


if __name__ == "__main__":
    region, buckets = read_sam_config()
    empty_s3_buckets(buckets, region)
    