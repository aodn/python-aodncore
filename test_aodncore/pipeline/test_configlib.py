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
        'error_uri': 'file:///tmp/probably/doesnt/exist/error',
        'opendap_root': 'http://opendap.example.com',
        'processing_dir': '/tmp/probably/doesnt/exist/processing',
        'upload_uri': 'file:///tmp/probably/doesnt/exist/upload',
        'wfs_url': 'http://geoserver.example.com/geoserver/wfs',
        'wfs_version': '1.0.0',
        'wip_dir': '/tmp/probably/doesnt/exist/wip',
        'platform_category_vocab_url': '/tmp/probably/doesnt/exist/aodn_aodn-platform-category-vocabulary.rdf',
        'platform_vocab_url': '/tmp/probably/doesnt/exist/aodn_aodn-platform-vocabulary.rdf'
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
        },
        "success_exit_policies": [
            "DELETE_SAME_NAME_FROM_ERROR_STORE"
        ],
        "error_exit_policies": [
        ]
    },
    "SOOP_DU_JOUR": {
        "path": [
            "SOOP/DU/JOUR",
            "SOOP/A/L/OIGNON"
        ],
        "handler": "DummyHandler",
        "params": {
        }
    }
}


class TestLazyConfigManager(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_celery_application(self):
        app = self.config.celery_application
        self.assertIsInstance(app, Celery)

    def test_celery_routes(self):
        expected_routes = {
            'tasks.ANMN_QLD_XXXX': {'queue': 'ANMN_QLD_XXXX', 'routing_key': 'ANMN_QLD_XXXX'},
            'tasks.SOOP_DU_JOUR': {'queue': 'SOOP_DU_JOUR', 'routing_key': 'SOOP_DU_JOUR'}
        }
        self.assertDictEqual(expected_routes, self.config.celery_routes)

    def test_watchservice_logging_config(self):
        watchservice_logging_config = self.config.watchservice_logging_config

        self.assertIn('watchservice_handler', watchservice_logging_config['handlers'].keys())

    def test_get_worker_logging_config(self):
        worker_logging_config = self.config.get_worker_logging_config('tasks.ANMN_QLD_XXXX')
        expected_logging_handlers = ['tasks.ANMN_QLD_XXXX_handler']
        self.assertCountEqual(expected_logging_handlers, worker_logging_config['handlers'].keys())

    def test_watch_directory_map(self):
        expected_map = {
            os.path.join(self.config.pipeline_config['watch']['incoming_dir'], 'ANMN/QLD/XXXX'): 'ANMN_QLD_XXXX',
            os.path.join(self.config.pipeline_config['watch']['incoming_dir'], 'SOOP/DU/JOUR'): 'SOOP_DU_JOUR',
            os.path.join(self.config.pipeline_config['watch']['incoming_dir'], 'SOOP/A/L/OIGNON'): 'SOOP_DU_JOUR'
        }
        self.assertDictEqual(expected_map, self.config.watch_directory_map)


class TestConfig(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_load_pipeline_config(self):
        pipeline_conf_file = os.path.join(CONF_ROOT, 'pipeline.conf')
        config = load_pipeline_config(pipeline_conf_file)
        self.assertDictEqual(REFERENCE_PIPELINE_CONFIG, config)

        nonexistent_config_file = get_nonexistent_path()
        with self.assertRaises(InvalidConfigError):
            _ = load_pipeline_config(nonexistent_config_file, envvar='')

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
