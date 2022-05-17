import os

import s3fs
from minio import Minio

from .path import add_final_slash_to_path


def make_s3_path(bucket_name, *args, include_s3_prefix=True,
                 include_final_slash=True):
    """Constructs s3 path.

    Every arg passed is joined inserting '/' as needed.

    Parameters
    ----------
    bucket_name : str
        Bucket name.

    args : Extra args passed to path.
        Every arg passed is joined inserting '/' as needed.

    include_s3_prefix : bool, default=True
        Whether or not the returned path starts with "s3://".

    include_final_slash : bool, default=True
        Whether or not the returned path contains a slash as the final
        character.
    """
    if include_s3_prefix:
        base = "s3://{}".format(bucket_name)
    else:
        base = bucket_name
    path = os.path.join(base, *args)

    if include_final_slash:
        path = add_final_slash_to_path(path)

    return path


def make_s3_filesystem(client_args, verify=False, annon=False, use_ssl=False,
                       **kwargs):
    """Makes s3 filesystem.

    Parameters
    ----------
    client_args : `celery_app.client_args.ClientArgs`
        Instance from :class:`celery_app.client_args.ClientArgs`.

    """
    endpoint_url = client_args.get_s3_endpoint()

    # s3 filesystem client args.
    fs_client_args = {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": client_args.access_key,
        "aws_secret_access_key": client_args.secret_key,
        "verify": verify
    }

    fs = s3fs.S3FileSystem(anon=annon, use_ssl=use_ssl,
                           client_kwargs=fs_client_args, **kwargs)
    return fs


def create_minio_client(client_args):
    """Creates minio client.

    Parameters
    ----------
    client_args : `celery_app.client_args.ClientArgs`
        Instance from :class:`celery_app.client_args.ClientArgs`.
    """
    client_args = {
        'access_key': client_args.access_key,
        'secret_key': client_args.secret_key,
        'endpoint': client_args.s3_endpoint,
        'secure': client_args.secure
    }
    return Minio(**client_args)
