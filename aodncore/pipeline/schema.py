__all__ = [
    'LOGGING_CONFIG_SCHEMA',
    'PIPELINE_CONFIG_SCHEMA'
]

LOGGING_CONFIG_SCHEMA = {
    'type': 'object',
    'properties': {
        'version': {
            'type': 'integer',
            'enum': [1]
        },
        'filters': {'type': 'object'},
        'formatters': {'type': 'object'},
        'handlers': {'type': 'object'},
        'loggers': {'type': 'object'},
    },
    'required': ['version', 'formatters', 'handlers', 'loggers'],
    'additionalProperties': False
}

PIPELINE_CONFIG_SCHEMA = {
    'type': 'object',
    'properties': {
        'global': {
            'type': 'object',
            'properties': {
                'admin_recipients': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'archive_uri': {'type': 'string'},
                'error_dir': {'type': 'string'},
                'processing_dir': {'type': 'string'},
                'tmp_dir': {'type': 'string'},
                'upload_uri': {'type': 'string'},
                'wip_dir': {'type': 'string'}
            },
            'required': ['admin_recipients', 'archive_uri', 'error_dir', 'processing_dir', 'upload_uri', 'wip_dir'],
            'additionalProperties': False
        },
        'logging': {
            'type': 'object',
            'properties': {
                'level': {'$ref': '#/definitions/loggingLevel'},
                'lib_level': {'$ref': '#/definitions/loggingLevel'},
                'pipeline_format': {'type': 'string'},
                'log_root': {'type': 'string'},
                'watchservice_format': {'type': 'string'}
            },
            'required': ['level', 'pipeline_format', 'log_root', 'watchservice_format'],
            'additionalProperties': False
        },
        'mail': {
            'type': 'object',
            'properties': {
                'from': {'type': 'string'},
                'subject': {'type': 'string'},
                'smtp_server': {'type': 'string'},
                'smtp_user': {'type': 'string'},
                'smtp_pass': {'type': 'string'}
            },
            'required': ['from', 'subject', 'smtp_server', 'smtp_user', 'smtp_pass'],
            'additionalProperties': False
        },
        'pluggable': {
            'type': 'object',
            'properties': {
                'handlers_group': {'type': 'string'},
                'path_function_group': {'type': 'string'}
            },
            'required': ['handlers_group', 'path_function_group'],
            'additionalProperties': False
        },
        'talend': {
            'type': 'object',
            'properties': {
                'talend_log_dir': {'type': 'string'}
            },
            'required': ['talend_log_dir'],
            'additionalProperties': False
        },
        'templating': {
            'type': 'object',
            'properties': {
                'template_package': {'type': 'string'},
                'html_notification_template': {'type': 'string'},
                'text_notification_template': {'type': 'string'},
            },
            'required': ['template_package', 'html_notification_template', 'text_notification_template'],
            'additionalProperties': False
        },
        'watch': {
            'type': 'object',
            'properties': {
                'incoming_dir': {'type': 'string'},
                'logger_name': {'type': 'string'},
                'task_namespace': {'type': 'string'}
            },
            'required': ['incoming_dir', 'logger_name', 'task_namespace'],
            'additionalProperties': False
        }
    },
    'required': ['global', 'logging', 'mail', 'talend', 'templating', 'watch'],
    'additionalProperties': False,
    'definitions': {
        'loggingLevel': {
            'type': 'string',
            'enum': ['CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO', 'SYSINFO', 'DEBUG', 'NOTSET']
        }
    }
}
