import importlib.util
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import quote
import httpx

MODULE = Path(__file__).resolve().parents[3] / 'mineru/data/io/node_s3.py'
spec = importlib.util.spec_from_file_location('node_s3', MODULE)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


class SigningTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'NODE_SERVICE_URL': 'https://node.test', 'AUTH_API_KEY': 'test-secret',
                                         'AWS_S3_BUCKET_NAME': '', 'S3_BUCKET': ''})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.requests = []
        self.objects = {}
        self.real_client = httpx.Client
        self.transport = httpx.MockTransport(self.handle)
        self.factory = patch.object(api.httpx, 'Client', side_effect=lambda **kw: self.real_client(transport=self.transport, **kw))
        self.factory.start()
        self.addCleanup(self.factory.stop)
        self.client = api.SignedS3Client()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def handle(self, request):
        self.requests.append(request)
        if request.url.host == 'node.test':
            self.assertEqual(request.headers.get('Authorization'), 'Bearer test-secret')
            data = json.loads(request.content)
            if request.url.path.endswith('upload-url'):
                key = data['dirPath'] + '/' + data['fileName']
                url = 'https://bucket.s3.ap-southeast-1.amazonaws.com/' + quote(key, safe='/')
                return httpx.Response(200, json={'code': 200, 'data': {'key': key, 'fileUrl': url,
                    'uploadUrl': url + '?sig=test', 'method': 'PUT', 'headers': {'Content-Type': data['contentType'], 'Content-Length': str(data['size'])}}})
            return httpx.Response(200, json={'code': 200, 'data': {'downloadUrl': 'https://bucket.s3.ap-southeast-1.amazonaws.com/' + quote(data['key'], safe='/') + '?sig=test'}})
        self.assertNotIn('Authorization', request.headers)
        if request.method == 'PUT':
            self.objects[request.url.path] = request.content
            return httpx.Response(200)
        return httpx.Response(206 if request.headers.get('Range') else 200, content=b'data')

    def test_upload_returns_encoded_location_and_does_not_delete_local_file(self):
        file = self.root / '报告 #1.md'
        file.write_bytes(b'abc')
        result = self.client.upload_file(file, '', 'research-reports/parsed/报告 #1.md')
        self.assertIn('%20%231.md', result['fileUrl'])
        payload = json.loads(self.requests[0].content)
        self.assertEqual(payload['sha256'], hashlib.sha256(b'abc').hexdigest())
        self.assertTrue(payload['overwrite'])
        self.assertNotIn('ACL', payload)
        self.assertEqual(self.client.bucket, 'bucket')
        self.assertTrue(file.exists())

    def test_tree_moves_images_and_rewrites_markdown_and_json(self):
        (self.root / 'images').mkdir()
        (self.root / 'images' / 'a.png').write_bytes(b'png')
        (self.root / 'report.md').write_text('![图](images/a.png)', encoding='utf-8')
        (self.root / 'report_content_list.json').write_text('[{"img_path":"images/a.png"}]')
        result = api.upload_tree(self.client, self.root, 'research-reports/parsed/123', top_name_prefix='123_')
        image = result['images/a.png']['fileUrl']
        self.assertIn('/images/mineru/research-reports/parsed/123/images/a.png', image)
        self.assertIn(image.encode(), self.objects['/research-reports/parsed/123/123_report.md'])
        self.assertEqual(json.loads(self.objects['/research-reports/parsed/123/123_report_content_list.json'])[0]['img_path'], image)
        self.assertEqual((self.root / 'report.md').read_text(), '![图](images/a.png)')

    def test_range_read_and_literal_plus_percent_are_preserved(self):
        result = self.client.get_object(Bucket='bucket', Key='research/A+B %20.pdf', Range='bytes=2-5')
        self.assertEqual(result['Body'].read(), b'data')
        self.assertEqual(json.loads(self.requests[0].content)['key'], 'research/A+B %20.pdf')
        self.assertEqual(self.requests[1].headers['Range'], 'bytes=2-5')
        signed = api.signed_source_url('https://bucket.s3.ap-southeast-1.amazonaws.com/research/A+B%20%2520.pdf')
        self.assertIn('sig=test', signed)
        self.assertEqual(json.loads(self.requests[-1].content)['key'], 'research/A+B %20.pdf')

    def test_cross_bucket_access_is_rejected_before_object_request(self):
        with self.assertRaises(ValueError):
            self.client.get_object(Bucket='other', Key='research/a.pdf')
        self.assertEqual(len(self.requests), 1)

    def test_external_sources_unchanged_and_ambiguous_urls_rejected(self):
        self.assertEqual(api.signed_source_url('https://example.com/a.pdf'), 'https://example.com/a.pdf')
        with self.assertRaises(ValueError):
            api.signed_source_url('https://bucket.s3.ap-southeast-1.amazonaws.com/a#b.pdf')

    def test_buffer_writes_preserve_exact_key(self):
        result = self.client.put_object(Bucket='bucket', Key='images/mineru/a.png', Body=b'png')
        self.assertEqual(result['key'], 'images/mineru/a.png')
        self.assertEqual(self.objects['/images/mineru/a.png'], b'png')

    def load_worker_function(self, filename, function):
        # Exercise real worker mapping without loading ML models or connecting databases.
        import ast
        import __future__
        from unittest.mock import Mock
        path = MODULE.parents[3] / 'projects/mongodb_worker' / filename
        tree = ast.parse(path.read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == function)
        namespace = dict(Path=Path, make_s3=lambda: self.client, S3_PREFIX='research-reports/parsed',
                         S3_BUCKET='', upload_tree=api.upload_tree, logger=Mock(),
                         IMAGE_EXTENSIONS=api.IMAGE_EXTENSIONS)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec',
                     flags=__future__.annotations.compiler_flag), namespace)
        return namespace[function]

    def test_report_and_generic_workers_save_permanent_locations(self):
        (self.root / 'report.md').write_text('# report')
        (self.root / 'report_content_list.json').write_text('[]')
        report = self.load_worker_function('report_worker.py', 'upload_result')(self.root, '123')
        self.assertEqual(report['bucket'], 'bucket')
        self.assertTrue(report['markdown'].endswith('/123/123_report.md'))
        self.assertNotIn('?', report['markdown'])
        generic = self.load_worker_function('worker.py', 'upload_to_s3')(self.root, '456')
        self.assertEqual(generic['markdown'], 'research-reports/parsed/456/report.md')
        self.assertEqual(generic['s3_bucket'], 'bucket')

    def test_pg_worker_moves_image_source_into_images_directory(self):
        source = self.root / 'source.png'
        source.write_bytes(b'png')
        output = self.root / 'output'
        output.mkdir()
        (output / 'report.md').write_text('# report')
        result = self.load_worker_function('worker_with_pg.py', 'upload_assets_to_s3')(output, '123', source)
        self.assertEqual(result['source'], 'images/mineru/research-reports/parsed/docs/123/source.png')

    def test_transient_upload_failure_refreshes_grant_and_reopens_body(self):
        original = self.handle
        attempts = []
        def handler(request):
            if request.method == 'PUT':
                attempts.append(request.content)
                if len(attempts) == 1:
                    return httpx.Response(503)
            return original(request)
        self.transport = httpx.MockTransport(handler)
        with patch.object(api.time, 'sleep'):
            self.client.put_object(Bucket='bucket', Key='research/a.pdf', Body=b'pdf')
        self.assertEqual(attempts, [b'pdf', b'pdf'])
        self.assertEqual(sum(r.url.host == 'node.test' for r in self.requests), 2)
