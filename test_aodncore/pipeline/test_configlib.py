import os
import tempfile

from celery import Celery

from aodncore.pipeline.configlib import CustomParser, load_pipeline_config, load_watch_config
from aodncore.pipeline.exceptions import InvalidConfigError
from test_aodncore.testlib import BaseTestCase, get_nonexistent_path

TEST_ROOT = os.path.dirname(__file__)

REFERENCE_PIPELINE_CONFIG = {
    'global': {
        'admin_recipients': ['unittest:dummy'],
        'archive_uri': 'file:///tmp/probably/doesnt/exist/archive',
        'error_dir': '/tmp/probably/doesnt/exist/error',
        'processing_dir': '/tmp/probably/doesnt/exist/processing',
        'upload_uri': 'file:///tmp/probably/doesnt/exist/upload',
        'wip_dir': '/tmp/probably/doesnt/exist/wip'
    },
    'logging': {
        'level': 'INFO',
        'pipeline_format': '%(asctime)s %(levelname)s %(name)s[%(celery_task_id)s] %(message)s',
        'log_root': '',
        'watchservice_format': '%(asctime)s %(levelname)s [%(name)s] %(message)s'
    },
    'mail': {
        'from': 'info@aodn.org.au',
        'subject': 'aodn-pipeline unit testing',
        'smtp_server': '028fbd24-a700-40af-95c9-155c3b023a64',
        'smtp_user': 'ACCESS_KEY',
        'smtp_pass': 'SECRET_KEY'
    },
    "pluggable": {
        "handlers_group": "pipeline.handlers",
        "path_function_group": "pipeline.path_functions"
    },
    'talend': {'talend_log_dir': '/tmp/probs/doesnt/exist/process'},
    'templating': {
        'template_package': 'aodncore.pipeline',
        'html_notification_template': 'notify.html.j2',
        'text_notification_template': 'notify.txt.j2'
    },
    'watch': {
        'incoming_dir': '/tmp/probably/doesnt/exist/incoming',
        'logger_name': 'watchservice',
        'task_namespace': 'tasks'
    }

}

REFERENCE_WATCH_CONFIG = {
    "ANMN_QLD_XXXX": {
        "path": [
            "ANMN/QLD/XXXX"
        ],
        "handler": "DummyHandler",
        "params": {
            "include_regexes": [
                ".*"
            ],
            "check_params": {
                "checks": [
                    "cf"
                ]
            },
            "notify_list": [
                "4.XXXX"
            ]
        }
    }
}

REFERENCE_WATCH_DIRECTORY_MAP = {
    '/tmp/probably/doesnt/exist/incoming/ANMN/QLD/XXXX': 'ANMN_QLD_XXXX'
}


class TestCustomParser(BaseTestCase):
    def setUp(self):
        super(TestCustomParser, self).setUp()
        parser = CustomParser()
        parser.add_section('section1')
        parser.add_section('section2')
        parser.add_section('section3')
        parser.add_section('section4')
        parser.set('DEFAULT', 'defaultitem', 'foo')
        parser.set('section1', 'item1', 'bar')
        parser.set('section2', 'item2', '123456')
        parser.set('section3', 'item3', 'baz')
        parser.set('section3', 'item4', '%(defaultitem)s AND %(item3)s')
        parser.set('section4', 'item5', 'value1,value 2, value3 , value4,,value 5, ,value6')

        _, self.temp_config_file = tempfile.mkstemp(dir=self.temp_dir)
        with open(self.temp_config_file, 'wb') as f:
            parser.write(f)

        self.parser = CustomParser()
        self.parser.read(self.temp_config_file)

    def test_as_parser(self):
        new_parser = CustomParser()
        new_parser.read(self.temp_config_file)

        self.assertDictEqual(new_parser._sections, self.parser._sections)

    def test_as_dict(self):
        reference_dict = {
            'section1': {
                'defaultitem': 'foo', 'item1': 'bar'
            },
            'section2': {
                'item2': '123456', 'defaultitem': 'foo'
            },
            'section3': {
                'defaultitem': 'foo', 'item4': 'foo AND baz', 'item3': 'baz'
            },
            'section4': {
                'item5': 'value1,value 2, value3 , value4,,value 5, ,value6', 'defaultitem': 'foo'
            },

        }

        dict_representation = self.parser.as_dict()
        self.assertDictEqual(reference_dict, dict_representation)

    def test_getlist(self):
        reference_list = ['value1', 'value 2', 'value3', 'value4', 'value 5', 'value6']
        reference_list_spaces = ['value1,value', '2,', 'value3', ',', 'value4,,value', '5,', ',value6']

        list_item = self.parser.getlist('section4', 'item5')
        self.assertListEqual(reference_list, list_item)

        list_item_spaces = self.parser.getlist('section4', 'item5', delimiter=' ')
        self.assertListEqual(reference_list_spaces, list_item_spaces)


class TestLazyConfigManager(BaseTestCase):
    def setUp(self):
        super(TestLazyConfigManager, self).setUp()

    def test_celery_application(self):
        app = self.config.celery_application
        self.assertIsInstance(app, Celery)

    def test_logging_config(self):
        self.assertIsNone(self.config._logging_config)
        logging_config = self.config.logging_config
        pass

    def test_watch_directory_map(self):
        self.assertIsNone(self.config._watch_directory_map)
        self.assertDictEqual(REFERENCE_WATCH_DIRECTORY_MAP, self.config.watch_directory_map)

    def test_purge_lazy_properties(self):
        _ = self.config.celery_application
        _ = self.config.pipeline_config
        _ = self.config.watch_config
        self.assertIsNotNone(self.config._celery_application)
        self.assertIsNotNone(self.config._pipeline_config)
        self.assertIsNotNone(self.config._watch_config)

        self.config.purge_lazy_properties()

        self.assertIsNone(self.config._celery_application)
        self.assertIsNone(self.config._pipeline_config)
        self.assertIsNone(self.config._watch_config)


class TestConfig(BaseTestCase):
    def test_load_pipeline_config(self):
        config = load_pipeline_config()
        self.assertDictEqual(REFERENCE_PIPELINE_CONFIG, config)

        nonexistent_config_file = get_nonexistent_path()
        with self.assertRaises(InvalidConfigError):
            _ = load_pipeline_config(nonexistent_config_file, envvar=None)

    def test_load_watch_config(self):
        config = load_watch_config()
        self.assertDictEqual(REFERENCE_WATCH_CONFIG, config)
