import pandas as pd

class BronzeToSilverProcessor:
    def __init__(self, s3_service, source_bucket, target_bucket):
        self.s3 = s3_service
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket

    def process(self, prefix):
        object_keys = self.s3.list_json_objects(self.source_bucket, prefix)
        if not object_keys:
            raise Exception(f"No .json files found under {prefix}")

        json_objects = [self.s3.load_json(self.source_bucket, key) for key in object_keys]
        df = self.normalize_records(json_objects)

        output_key = "silver/movies.csv"
        self.s3.save_csv(self.target_bucket, output_key, df.to_csv(index=False))

        return len(df)

    def normalize_records(self, json_objects):
        records = []
        for obj in json_objects:
            flattened = {
                k.lower(): v for k, v in obj.items()
                if isinstance(v, (str, int, float, bool, list))
            }
            records.append(flattened)
        self.s3.logger.info(f"Normalized {len(records)} records from JSON objects")
        return pd.DataFrame(records)
