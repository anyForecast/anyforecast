import io
import json

import awswrangler as wr


class PandasWriter:

    def __init__(self):
        pass

    def parquet(self, data, path, fs, **kwargs):
        self._set_awswrangler_endpoint(fs)
        wr.s3.to_parquet(df=data, path=path, dataset=True, mode="overwrite",
                         **kwargs)

    def _set_awswrangler_endpoint(self, fs):
        endpoint_url = fs.client_kwargs['endpoint_url']
        wr.config.s3_endpoint_url = endpoint_url


class SparkWriter:
    def __init__(self):
        pass

    def parquet(self, data):
        pass  # self.data.write.parquet...


class JsonWriter:
    def __init__(self, data):
        self.data = data

    def json(self, path, minio_client, bucket_name):
        self._put_json(minio_client, path, bucket_name)

    def _put_json(self, minio_client, path, bucket_name):
        """
        jsonify a dict and write it as object to the bucket
        """
        # Prepare data and corresponding data stream
        data = json.dumps(self.data).encode("utf-8")
        data_stream = io.BytesIO(data)
        data_stream.seek(0)

        # Put data as object into the bucket
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=path,
            data=data_stream, length=len(data),
            content_type="application/json"
        )
