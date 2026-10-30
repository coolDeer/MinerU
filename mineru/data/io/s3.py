# Copyright (c) Opendatalab. All rights reserved.
from .node_s3 import SignedS3Client

from ..io.base import IOReader, IOWriter


class S3Reader(IOReader):
    def __init__(
        self,
        bucket: str,
        ak: str = '',
        sk: str = '',
        endpoint_url: str = '',
        addressing_style: str = 'auto',
    ):
        """s3 reader client.

        Args:
            bucket (str): bucket name
            ak (str): access key
            sk (str): secret key
            endpoint_url (str): endpoint url of s3
            addressing_style (str, optional): Defaults to 'auto'. Other valid options here are 'path' and 'virtual'
            Legacy credentials/endpoint arguments are ignored; configure NODE_SERVICE_URL and AUTH_API_KEY.
        """
        self._bucket = bucket
        self._s3_client = SignedS3Client()

    def read(self, key: str) -> bytes:
        """Read the file.

        Args:
            path (str): file path to read

        Returns:
            bytes: the content of the file
        """
        return self.read_at(key)

    def read_at(self, key: str, offset: int = 0, limit: int = -1) -> bytes:
        """Read at offset and limit.

        Args:
            path (str): the path of file, if the path is relative path, it will be joined with parent_dir.
            offset (int, optional): the number of bytes skipped. Defaults to 0.
            limit (int, optional): the length of bytes want to read. Defaults to -1.

        Returns:
            bytes: the content of file
        """
        if limit == 0:
            return b''
        if offset < 0 or limit < -1:
            raise ValueError('Invalid byte range')
        if limit > -1:
            range_header = f'bytes={offset}-{offset+limit-1}'
            res = self._s3_client.get_object(
                Bucket=self._bucket, Key=key, Range=range_header
            )
        else:
            res = self._s3_client.get_object(
                Bucket=self._bucket, Key=key, Range=f'bytes={offset}-'
            )
        return res['Body'].read()


class S3Writer(IOWriter):
    def __init__(
        self,
        bucket: str,
        ak: str = '',
        sk: str = '',
        endpoint_url: str = '',
        addressing_style: str = 'auto',
    ):
        """s3 reader client.

        Args:
            bucket (str): bucket name
            ak (str): access key
            sk (str): secret key
            endpoint_url (str): endpoint url of s3
            addressing_style (str, optional): Defaults to 'auto'. Other valid options here are 'path' and 'virtual'
            Legacy credentials/endpoint arguments are ignored; configure NODE_SERVICE_URL and AUTH_API_KEY.
        """
        self._bucket = bucket
        self._s3_client = SignedS3Client()

    def write(self, key: str, data: bytes):
        """Write file with data.

        Args:
            path (str): the path of file, if the path is relative path, it will be joined with parent_dir.
            data (bytes): the data want to write
        """
        self._s3_client.put_object(Bucket=self._bucket, Key=key, Body=data)
