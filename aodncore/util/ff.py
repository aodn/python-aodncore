"""This module provides utility functions specifically related to frictionless framework / tableschema.
"""

from tableschema import Schema
from ..pipeline.exceptions import InvalidSchemaError

__all__ = [
    'get_tableschema_descriptor',
    'get_field_type'
]


def get_tableschema_descriptor(obj, name):
    """Convenience function to return a valid tableschema definition.

    :param obj: A dict that is either the desired object or the parent of the desired object
    :param name: A string containing name of the nested object (eg. 'schema')
    :return: A valid tableschema definition
    """
    s = Schema(obj.get(name, obj))
    if not s.valid:
        raise InvalidSchemaError('Schema definition does not meet the tableschema standard')
    else:
        return s.descriptor


def get_field_type(field):
    """Find a field by name in translation table and return associated field type.

    :param field: A string containing the tableschema definition field type.
    :return: A string containing the associated postgresql field type if different
        - otherwise the passed in field param.
    """
    translations = {
        'integer': 'int',
        'string': 'varchar',
        'any': 'varchar',
        'number': 'numeric',
        'datetime': 'timestamp',
        'date': 'date'
    }
    return translations.get(field, field)

