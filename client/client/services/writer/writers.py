import io
import json

import pyarrow as pa
from pyarrow import parquet as pq


class PandasWriter:

    def __init__(self, data):
        self.data = data

    def parquet(self, path, fs, **kwargs):
        table = pa.Table.from_pandas(self.data)
        pq.write_to_dataset(table, path, filesystem=fs, use_dictionary=True,
                            compression="snappy", version="2.4",
                            **kwargs)


class SparkWriter:
    def __init__(self, data):
        self.data = data

    def parquet(self):
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
