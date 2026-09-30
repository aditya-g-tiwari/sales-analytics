import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('loader', Path(__file__).resolve().parents[1] / 'scripts/load_snowflake.py')
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)

class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.m = {'version': 1, 'status': 'complete', 'run_id': 'a'*32,
                  's3_base_path': loader.BASE, 'started_at': '2026-09-30T10:00:00+00:00',
                  'completed_at': '2026-09-30T10:01:00+00:00', 'tables': [
                      {'source_table': t, 'relative_path': f'{p}/run_id={"a"*32}/', 'row_count': 0}
                      for t,p in loader.TABLES.items()]}
    def test_empty_complete_snapshot(self):
        self.assertEqual(loader.validate_manifest(self.m), self.m)
    def test_rejects_unfinished(self):
        self.m['status'] = 'running'
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_missing_and_duplicate_sources(self):
        self.m['tables'][0] = copy.deepcopy(self.m['tables'][1])
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_path_injection(self):
        self.m['tables'][0]['relative_path'] = "leads/';DROP TABLE test;--"
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_wrong_bucket(self):
        self.m['s3_base_path'] = 's3://unexpected/raw/'
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_invalid_counts(self):
        for count in (-1, True, '100', 1.5):
            with self.subTest(count=count):
                self.m['tables'][0]['row_count'] = count
                with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_bad_run_id(self):
        self.m['run_id'] = '../bad'
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)
    def test_rejects_reversed_time(self):
        self.m['completed_at'] = '2026-09-29T10:00:00+00:00'
        with self.assertRaises(ValueError): loader.validate_manifest(self.m)

if __name__ == '__main__': unittest.main()
