import inspect
import os
from collections import OrderedDict

from celery import Celery

from aodncore.pipeline.configlib import load_pipeline_config, load_trigger_config, load_watch_config
from aodncore.pipeline.exceptions import InvalidConfigError
from aodncore.testlib import BaseTestCase, conf, get_nonexistent_path

CONF_ROOT = os.path.dirname(inspect.getfile(conf))
TEST_ROOT = os.path.dirname(__file__)

REFERENCE_PIPELINE_CONFIG = {
    'global': {
        'admin_recipients': ['unittest:dummy'],
        'archive_uri': 'file:///tmp/probably/doesnt/exist/archive',
        'error_dir': '/tmp/probably/doesnt/exist/error',
        'opendap_root': 'http://opendap.example.com',
        'processing_dir': '/tmp/probably/doesnt/exist/processing',
        'upload_uri': 'file:///tmp/probably/doesnt/exist/upload',
        'wip_dir': '/tmp/probably/doesnt/exist/wip'
    },
    'logging': {
        'level': 'SYSINFO',
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
        "path_function_group": "pipeline.path_functions",
        "module_versions_group": "pipeline.module_versions"
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

REFERENCE_TRIGGER_CONFIG = OrderedDict([
    ("zzz_my_test_harvester", OrderedDict([
        (
            "exec",
            'echo zzz_my_test_harvester --context_param paramFile="/usr/local/talend/jobs/param_file.conf" --context_param base=%{base} --context_param fileList=%{file_list} --context_param logDir=%{log_dir}'
        ),
        (
            "events", [
                OrderedDict([
                    ("regex", [".*"])
                ])
            ]
        )
    ])),
    ("aaa_my_test_harvester", OrderedDict([
        (
            "exec",
            'echo aaa_my_test_harvester --context_param paramFile="/usr/local/talend/jobs/param_file.conf" --context_param base=%{base} --context_param fileList=%{file_list} --context_param logDir=%{log_dir}'
        ),
        (
            "events", [
                OrderedDict([
                    ("regex", [".*"]),
                    ("extra_params", "--collection my_test_collection")
                ]),
                OrderedDict([
                    ("regex", [".*"])
                ])
            ]
        ),
    ])),
    ("mmm_my_test_harvester", OrderedDict([
        (
            "exec",
            'echo mmm_my_test_harvester --context_param paramFile="/usr/local/talend/jobs/param_file.conf" --context_param base=%{base} --context_param fileList=%{file_list} --context_param logDir=%{log_dir}'
        ),
        (
            "events", [
                OrderedDict([
                    ("regex", [".*"])
                ])
            ]
        )
    ]))
])

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
            "notify_params": {
                "error_notify_list": [
                    "4.XXXX"
                ]
            }
        }
    }
}

REFERENCE_WATCH_DIRECTORY_MAP = {
    '/tmp/probably/doesnt/exist/incoming/ANMN/QLD/XXXX': 'ANMN_QLD_XXXX'
}


class TestLazyConfigManager(BaseTestCase):
    def setUp(self):
        super(TestLazyConfigManager, self).setUp()

    def test_celery_application(self):
        app = self.config.celery_application
        self.assertIsInstance(app, Celery)

    def test_logging_config(self):
        self.assertIsNone(self.config._worker_logging_config)
        logging_config = self.config.worker_logging_config
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
    def setUp(self):
        super(TestConfig, self).setUp()

    def test_load_pipeline_config(self):
        pipeline_conf_file = os.path.join(CONF_ROOT, 'pipeline.conf')
        config = load_pipeline_config(pipeline_conf_file)
        self.assertDictEqual(REFERENCE_PIPELINE_CONFIG, config)

        nonexistent_config_file = get_nonexistent_path()
        with self.assertRaises(InvalidConfigError):
            _ = load_pipeline_config(nonexistent_config_file, envvar=None)

    def test_load_trigger_config(self):
        trigger_conf_file = os.path.join(CONF_ROOT, 'trigger.conf')
        config = load_trigger_config(trigger_conf_file)
        self.assertIsInstance(config, OrderedDict)
        self.assertDictEqual(REFERENCE_TRIGGER_CONFIG, config)
        self.assertListEqual(list(REFERENCE_TRIGGER_CONFIG.keys()), list(config.keys()))

    def test_load_watch_config(self):
        watch_conf_file = os.path.join(CONF_ROOT, 'watches.conf')
        config = load_watch_config(watch_conf_file)
        self.assertDictEqual(REFERENCE_WATCH_CONFIG, config)
