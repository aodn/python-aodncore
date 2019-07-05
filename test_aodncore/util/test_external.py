import uuid

from aodncore.testlib import BaseTestCase
from aodncore.util.external import classproperty, lazyproperty


class _DummyClass(object):
    @lazyproperty
    def lazy_uuid(self):
        return str(uuid.uuid4())

    @classproperty
    def class_value(self):
        return 'qwerty123'


class TestUtilExternal(BaseTestCase):
    def setUp(self):
        self.dummy = _DummyClass()

    def test_classproperty(self):
        self.assertEqual(_DummyClass.class_value, 'qwerty123')

    def test_lazyproperty_get(self):
        first = self.dummy.lazy_uuid
        second = self.dummy.lazy_uuid
        self.assertEqual(first, second)

    def test_lazyproperty_set(self):
        with self.assertRaises(AttributeError):
            self.dummy.lazy_uuid = 'abc123'

    def test_lazyproperty_delete(self):
        with self.assertRaises(AttributeError):
            del self.dummy.lazy_uuid
