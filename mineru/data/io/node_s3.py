"""S3 compatibility adapter backed exclusively by the trusted Node signing API."""
import hashlib
import io
import mimetypes
import os
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp', '.avif'}


class SignedS3Client:
    def __init__(self):
        self.base_url = (os.getenv('NODE_SERVICE_URL') or os.getenv('MEETING_TASK_BASE_URL', '')).rstrip('/')
        endpoint = urlsplit(self.base_url)
        if endpoint.scheme not in ('http', 'https') or not endpoint.hostname or endpoint.username or endpoint.password:
            raise ValueError('Configure NODE_SERVICE_URL for Node signed S3 access')
        key = os.getenv('AUTH_API_KEY', '').strip()
        if not key and endpoint.hostname not in ('localhost', '127.0.0.1', '::1'):
            raise ValueError('AUTH_API_KEY is required for remote Node access')
        self.auth = {'Authorization': 'Bearer ' + key} if key else {}
        self.bucket = os.getenv('AWS_S3_BUCKET_NAME') or os.getenv('S3_BUCKET', '')

    def _grant(self, operation, payload):
        try:
            with httpx.Client(timeout=60, follow_redirects=False) as client:
                response = client.post(self.base_url + '/v1/api/files/' + operation, json=payload, headers=self.auth)
                response.raise_for_status()
                value = response.json()
                if response.status_code != 200 or not isinstance(value, dict) or value.get('code') != 200 or not isinstance(value.get('data'), dict):
                    raise ValueError('Invalid Node signing response')
                return value['data']
        except httpx.HTTPError as error:
            status = getattr(getattr(error, 'response', None), 'status_code', None)
            raise RuntimeError(f'Node signing failed (HTTP {status or "transport"})') from None

    def _url(self, url, bucket=''):
        parsed = urlsplit(url)
        host = parsed.hostname or ''
        if parsed.scheme != 'https' or not host.endswith('.amazonaws.com') or parsed.username or parsed.password or '.s3' not in host:
            raise ValueError('Invalid signed S3 endpoint')
        actual_bucket = host.split('.s3', 1)[0]
        expected_bucket = bucket or self.bucket
        if expected_bucket and expected_bucket != actual_bucket:
            raise ValueError('Requested bucket differs from Node configured bucket')
        self.bucket = actual_bucket
        return url

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        path = Path(filename)
        size = path.stat().st_size
        with path.open('rb') as stream:
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        return self._upload(lambda: path.open('rb'), size, digest.hexdigest(), bucket, key, ExtraArgs)

    def put_object(self, *, Bucket, Key, Body):
        return self._upload(lambda: io.BytesIO(Body), len(Body), hashlib.sha256(Body).hexdigest(), Bucket, Key)

    def _upload(self, open_body, size, checksum, bucket, key, extra=None):
        if not 0 < size <= 1024 ** 3:
            raise ValueError('Upload size must be 1 byte to 1 GiB')
        extension = Path(key).suffix.lower()
        directory, separator, filename = key.rpartition('/')
        if not separator:
            raise ValueError('An allowed business directory is required')
        mime = {'.md': 'text/markdown', '.json': 'application/json', '.svg': 'image/svg+xml'}.get(extension)
        mime = mime or mimetypes.guess_type(filename)[0]
        if not mime:
            raise ValueError('Unsupported file extension for signed uploads')
        for attempt in range(3):
            # Fresh grants on retries; overwrite preserves worker replay semantics.
            grant = self._grant('upload-url', {'dirPath': directory, 'fileName': filename,
                'contentType': mime, 'size': size, 'sha256': checksum, 'overwrite': True})
            url = self._url(grant['uploadUrl'], bucket)
            self._url(grant['fileUrl'], bucket)
            headers = grant['headers']
            if grant.get('method') != 'PUT' or not isinstance(headers, dict) or any(k.lower() in ('authorization', 'cookie', 'x-api-key') for k in headers):
                raise ValueError('Invalid upload grant headers')
            try:
                with open_body() as body, httpx.Client(timeout=1800, trust_env=False, follow_redirects=False) as client:
                    response = client.put(url, content=body, headers=headers)
                    response.raise_for_status()
                    if response.status_code != 200:
                        raise ValueError('Unexpected upload status')
                return grant
            except httpx.HTTPError as error:
                status = getattr(getattr(error, 'response', None), 'status_code', None)
                if attempt < 2 and (isinstance(error, httpx.TransportError) or status in (429, 500, 502, 503, 504)):
                    time.sleep(0.2 * 2 ** attempt)
                    continue
                raise RuntimeError(f'S3 upload failed (HTTP {status or "transport"})') from None

    def download_url(self, key, bucket=''):
        grant = self._grant('download-url', {'key': key})
        return self._url(grant['downloadUrl'], bucket)

    def get_object(self, *, Bucket, Key, Range=None):
        url = self.download_url(Key, Bucket)
        try:
            with httpx.Client(timeout=300, trust_env=False, follow_redirects=False) as client:
                response = client.get(url, headers={'Range': Range} if Range else {})
                response.raise_for_status()
                if response.status_code not in (200, 206):
                    raise ValueError('Unexpected download status')
                return {'Body': io.BytesIO(response.content)}
        except httpx.HTTPError:
            raise RuntimeError('S3 download failed') from None

    def download_file(self, bucket, key, filename):
        url = self.download_url(key, bucket)
        try:
            with httpx.stream('GET', url, timeout=300, trust_env=False, follow_redirects=False) as response:
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError('Unexpected download status')
                with open(filename, 'wb') as target:
                    for chunk in response.iter_bytes():
                        target.write(chunk)
        except httpx.HTTPError:
            raise RuntimeError('S3 download failed') from None


def signed_source_url(url):
    """Sign permanent AWS S3 URLs; leave external and already signed URLs unchanged."""
    parsed = urlsplit(url.replace(' ', '%20'))
    if parsed.scheme == 's3':
        return SignedS3Client().download_url(unquote(parsed.path.lstrip('/')), parsed.netloc)
    host = parsed.hostname or ''
    if host.endswith('.amazonaws.com') and '.s3' in host and not parsed.query:
        if parsed.fragment:
            raise ValueError('Ambiguous unencoded S3 fragment; supply an encoded permanent URL')
        return SignedS3Client().download_url(unquote(parsed.path.lstrip('/')), host.split('.s3', 1)[0])
    return url


def upload_tree(client, root, prefix, bucket='', top_name_prefix=''):
    """Publish images first, then rewrite document references to actual returned URLs."""
    import json
    import re
    import tempfile
    root = Path(root)
    files = sorted(p for p in root.rglob('*') if p.is_file())
    uploaded = {}
    image_urls = {}
    for file in sorted(files, key=lambda p: p.suffix.lower() not in IMAGE_EXTENSIONS):
        relative = file.relative_to(root).as_posix()
        name = relative
        if '/' not in relative and top_name_prefix and not relative.startswith(top_name_prefix):
            name = top_name_prefix + relative
        key = prefix + '/' + name
        if file.suffix.lower() in IMAGE_EXTENSIONS and not key.startswith('images/'):
            key = 'images/mineru/' + key
        payload = None
        def image_url(value):
            if not isinstance(value, str):
                return value
            # Resolve relative to the document, never guess a different uploaded object.
            local = (file.parent / unquote(value)).resolve()
            return image_urls.get(local, value)
        if file.suffix.lower() == '.md':
            text = file.read_text(encoding='utf-8')
            payload = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)',
                             lambda m: '![' + m[1] + '](' + image_url(m[2]) + ')', text).encode('utf-8')
        elif file.suffix.lower() == '.json':
            def rewrite(value):
                if isinstance(value, dict):
                    return {k: rewrite(v) for k, v in value.items()}
                if isinstance(value, list):
                    return [rewrite(v) for v in value]
                return image_url(value)
            payload = json.dumps(rewrite(json.loads(file.read_text(encoding='utf-8'))), ensure_ascii=False).encode('utf-8')
        if payload is None:
            result = client.upload_file(str(file), bucket, key)
        else:
            # Preserve original local artifacts for retries and downstream parsing.
            with tempfile.NamedTemporaryFile(suffix=file.suffix) as temp:
                temp.write(payload)
                temp.flush()
                result = client.upload_file(temp.name, bucket, key)
        uploaded[relative] = result
        if file.suffix.lower() in IMAGE_EXTENSIONS:
            image_urls[file.resolve()] = result['fileUrl']
    return uploaded


def download_source_file(url, destination, timeout=300):
    target = signed_source_url(url)
    try:
        with httpx.stream('GET', target, timeout=timeout, follow_redirects=True, trust_env=False) as response:
            response.raise_for_status()
            with open(destination, 'wb') as output:
                for chunk in response.iter_bytes():
                    output.write(chunk)
    except httpx.HTTPError:
        raise RuntimeError('Source download failed; check source access and Node signing configuration') from None
