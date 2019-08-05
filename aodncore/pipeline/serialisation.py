import importlib
import json
from datetime import date, datetime

from enum import Enum


def get_object_from_name(name, namespace):
    module = importlib.import_module(namespace)
    return getattr(module, name)


def datetime_to_string(dt):
    """Convert a date or datetime object to a string in ISO 8601 format

    :param dt: date or datetime instance
    :return: ISO 8601 formatted date time string
    """
    return dt.isoformat()


def string_to_datetime(s, string_format='%Y-%m-%dT%H:%M:%S.%f'):
    """Parse an ISO 8601 formatted string (as created by datetime_to_string) to a datetime instance

    :param s: input string
    :param string_format: format string passed to strptime
    :return: datetime instance
    """
    return datetime.strptime(s, string_format)


class PipelineJSONDecoder(json.JSONDecoder):
    """Custom JSONDecoder class to decode custom class objects that have been encoded by PipelineJSONEncoder

    Classes can control how they are decoded and "re-instantiated" by:
    1. implementing a `from_json` method; and
    2. ensuring that their `to_json` method encodes the instance in the follow structure:

        {
            '__decode__class__': 'ClassName',
            'data': {
                'attribute1': 'value1',
                'attribute2': 'value2
            }
        }
    Only classes and Enums encoded by PipelineJSONEncoder will be re-instantiated, others will fall back to the
    default JSONDecoder behaviour
    """

    def __init__(self, *args, **kwargs):
        kwargs['object_hook'] = self.object_hook
        super(PipelineJSONDecoder, self).__init__(*args, **kwargs)

    @staticmethod
    def object_hook(d):
        if '__pipeline_enum__' in d:
            name, member = d['__pipeline_enum__'].split('.')
            enum_class = get_object_from_name(name, d['__module__'])
            return getattr(enum_class, member)
        elif '__pipeline_datetime__' in d:
            return string_to_datetime(d['__pipeline_datetime__'])
        elif '__decode_class__' in d:
            decode_class = get_object_from_name(d['__decode_class__'], d['__module__'])
            return decode_class.from_json(d)

        return d


class PipelineJSONEncoder(json.JSONEncoder):
    """Custom JSONEncoder class to encode custom classes to support simple decoding

    Any class can define a `to_json` method in order to control it's own JSON representation.
    Enums are treated as a special exception, due to the specific behaviours around loading both the Enum class and also
    one of it's members. datetime objects are also handled as a special case, in order to enforce a consistent format.

    An additional implementation specific behaviour is that if an object is neither a valid "pipeline" object nor a type
    that the standard JSON decoder understands, the object is serialised as a string representation rather than the
    default behaviour raising a TypeError.
    """

    def default(self, o):
        if isinstance(o, Enum):
            return {'__pipeline_enum__': str(o), '__module__': o.__module__}
        elif isinstance(o, (datetime, date)):
            return {'__pipeline_datetime__': datetime_to_string(o)}
        elif hasattr(o, 'to_json'):
            return o.to_json()

        try:
            return super(PipelineJSONEncoder, self).default(self, o)
        except TypeError:
            return "UNSERIALISABLE({o})".format(o=repr(o))
