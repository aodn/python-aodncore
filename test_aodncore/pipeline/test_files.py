import os
import uuid
from collections import MutableSet, OrderedDict
from tempfile import mkstemp

from six import assertCountEqual
from six.moves import range

from aodncore.pipeline import (CheckResult, PipelineFileCollection, PipelineFile, PipelineFileCheckType,
                               PipelineFilePublishType)
from aodncore.pipeline.exceptions import MissingFileError
from aodncore.pipeline.steps import get_child_check_runner
from aodncore.testlib import BaseTestCase, get_nonexistent_path, mock
from aodncore.util import safe_copy_file
from test_aodncore import TESTDATA_DIR

GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


# noinspection PyAttributeOutsideInit
class TestPipelineFile(BaseTestCase):
    def setUp(self):
        super(TestPipelineFile, self).setUp()
        _, self.test_file = mkstemp(suffix='.nc', prefix=self.__class__.__name__)
        safe_copy_file(GOOD_NC, self.test_file, overwrite=True)

        self.pipelinefile = PipelineFile(self.test_file, dest_path=self.test_file + '.dest')

        self.pipelinefile_deletion = PipelineFile(get_nonexistent_path(), is_deletion=True)

    def tearDown(self):
        os.remove(self.test_file)

    def test_compliance_check(self):
        # Test compliance checking
        check_runner = get_child_check_runner(PipelineFileCheckType.NC_COMPLIANCE_CHECK, None, self.mock_logger,
                                              {'checks': ['cf']})
        check_runner.run(PipelineFileCollection(self.pipelinefile))
        assertCountEqual(self, dict(self.pipelinefile.check_result).keys(), ['compliant', 'errors', 'log'])

    def test_format_check(self):
        # Test file format checking
        check_runner = get_child_check_runner(PipelineFileCheckType.FORMAT_CHECK, None, self.mock_logger)
        check_runner.run(PipelineFileCollection(self.pipelinefile))
        assertCountEqual(self, dict(self.pipelinefile.check_result).keys(), ['compliant', 'errors', 'log'])

    def test_iter(self):
        dict_object = dict(self.pipelinefile)
        # Confirm that __iter__ returns a dict with the expected keys
        assertCountEqual(self, dict_object.keys(),
                         ['archive_path', 'check_passed', 'dest_path', 'file_checksum', 'check_log', 'check_type',
                          'is_archived', 'is_checked', 'is_deletion', 'is_harvested', 'is_stored', 'name',
                          'notification_result', 'pending_archive', 'pending_harvest_addition',
                          'pending_harvest_deletion', 'pending_store_addition', 'pending_store_deletion',
                          'publish_type', 'should_archive', 'should_harvest', 'should_store', 'src_path', ])

    def test_property_check_result(self):
        self.assertFalse(self.pipelinefile.is_checked)
        self.pipelinefile.check_result = CheckResult(True, False, None)
        self.assertTrue(self.pipelinefile.is_checked)

    def test_property_check_type(self):
        test_value = PipelineFileCheckType.FORMAT_CHECK
        self.pipelinefile.check_type = test_value
        self.assertEqual(self.pipelinefile.check_type, test_value)

    def test_property_dest_path(self):
        test_value = str(uuid.uuid4())
        self.pipelinefile.dest_path = test_value
        self.assertEqual(self.pipelinefile.dest_path, test_value)
        with self.assertRaises(ValueError):
            self.pipelinefile.dest_path = "/{uuid}".format(uuid=test_value)

    def test_property_publish_type(self):
        test_value = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.pipelinefile.publish_type = test_value
        self.assertEqual(self.pipelinefile.publish_type, test_value)

        with self.assertRaises(ValueError):
            self.pipelinefile.publish_type = 'invalid'

        with self.assertRaises(ValueError):
            self.pipelinefile_deletion.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD

    def test_property_should_archive(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        self.assertTrue(self.pipelinefile.should_archive)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE
        self.assertTrue(self.pipelinefile.should_archive)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.assertTrue(self.pipelinefile.should_archive)

    def test_property_should_store(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        self.assertTrue(self.pipelinefile.should_store)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_UPLOAD
        self.assertTrue(self.pipelinefile.should_store)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.assertTrue(self.pipelinefile.should_store)
        self.pipelinefile_deletion.publish_type = PipelineFilePublishType.DELETE_ONLY
        self.assertTrue(self.pipelinefile_deletion.should_store)
        self.pipelinefile_deletion.publish_type = PipelineFilePublishType.DELETE_UNHARVEST
        self.assertTrue(self.pipelinefile_deletion.should_store)

    def test_property_should_harvest(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ONLY
        self.assertTrue(self.pipelinefile.should_harvest)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_UPLOAD
        self.assertTrue(self.pipelinefile.should_harvest)
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.assertTrue(self.pipelinefile.should_harvest)

    def test_property_is_archived(self):
        self.assertFalse(self.pipelinefile.is_archived)
        self.pipelinefile.is_archived = True
        self.assertTrue(self.pipelinefile.is_archived)

    def test_property_is_stored(self):
        self.assertFalse(self.pipelinefile.is_stored)
        self.pipelinefile.is_stored = True
        self.assertTrue(self.pipelinefile.is_stored)

    def test_property_is_harvested(self):
        self.assertFalse(self.pipelinefile.is_harvested)
        self.pipelinefile.is_harvested = True
        self.assertTrue(self.pipelinefile.is_harvested)

    def test_file_callback(self):
        class TestCallbackContainer(object):
            def __init__(self):
                self.test_attribute = False
                self.test_kwargs = None

            def update_test_attribute(self, **kwargs):
                self.test_attribute = True
                self.test_kwargs = kwargs

        test_callback_instance = TestCallbackContainer()
        self.pipelinefile.file_update_callback = test_callback_instance.update_test_attribute

        self.pipelinefile.is_stored = True
        self.assertTrue(test_callback_instance.test_attribute)
        self.assertEqual(test_callback_instance.test_kwargs['name'], self.pipelinefile.name)


# noinspection PyAttributeOutsideInit
class TestPipelineFileCollection(BaseTestCase):
    def setUp(self):
        self.collection = PipelineFileCollection()

    def tearDown(self):
        del self.collection

    def test_abstract_class(self):
        self.assertIsInstance(self.collection, MutableSet)

    def test_invalid_types(self):
        class NothingClass(object):
            pass

        def nothing_function():
            pass

        # Test add with invalid types
        with self.assertRaises(TypeError):
            self.collection.add(1)
        with self.assertRaises(TypeError):
            self.collection.add(['foo', 'bar'])
        with self.assertRaises(TypeError):
            self.collection.add({'foo': 'bar'})
        with self.assertRaises(TypeError):
            self.collection.add(('foo', 'bar'))
        with self.assertRaises(TypeError):
            self.collection.add(NothingClass)
        with self.assertRaises(TypeError):
            self.collection.add(nothing_function)

        # Test update with invalid types
        with self.assertRaises(TypeError):
            self.collection.update('foobar')
        with self.assertRaises(TypeError):
            self.collection.update(1)
        with self.assertRaises(TypeError):
            self.collection.update(NothingClass)
        with self.assertRaises(TypeError):
            self.collection.update(nothing_function)
        with self.assertRaises(TypeError):
            self.collection.update({'foo': 'bar'})
        with self.assertRaises(TypeError):
            self.collection.update((NothingClass, nothing_function))

    def test_nonexistent_file(self):
        # Adding/discarding/membership testing filesystem paths
        with self.assertRaises(MissingFileError):
            self.collection.add(os.path.join('/nonexistent/path/with/a/{uuid}/in/the/middle'.format(uuid=uuid.uuid4())))

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_file_paths(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        self.collection.add(f1)
        self.collection.add(f2)
        self.assertIn(f1, self.collection)
        self.assertIn(f2, self.collection)

        self.collection.remove(f1)
        self.assertNotIn(f1, self.collection)

        self.collection.discard(f2)
        self.assertNotIn(f2, self.collection)

        self.collection.update([f1, f2])
        self.assertIn(f1, self.collection)
        self.assertIn(f2, self.collection)

        self.collection.clear()
        assertCountEqual(self, self.collection, set())

    def test_pipelinefile_objects(self):
        # Test add/discard/remove methods for PipelineFile instances
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.add(fileobj1)
        self.collection.add(fileobj2)
        self.assertIn(fileobj1, self.collection)
        self.assertIn(fileobj2, self.collection)

        self.collection.discard(fileobj1)
        self.assertNotIn(fileobj1, self.collection)

        self.collection.remove(fileobj2)
        self.assertNotIn(fileobj2, self.collection)

        self.collection.update([fileobj1, fileobj2])
        self.assertIn(fileobj1, self.collection)
        self.assertIn(fileobj2, self.collection)

        self.collection.clear()
        assertCountEqual(self, self.collection, set())

    def test_ordering(self):
        # Test that the order of elements is maintained and slicing returns expected results
        names = ['{0:04d}'.format(i) for i in range(10000)]
        for name in names:
            pf = PipelineFile(name, is_deletion=True)
            self.collection.add(pf)

        collection_names = [f.name for f in self.collection]

        self.assertListEqual(names, collection_names)
        collection_names.reverse()
        with self.assertRaises(AssertionError):
            self.assertListEqual(names, collection_names)

        names_slice = names[250:750]
        collection_slice = [f.name for f in self.collection[250:750]]
        self.assertListEqual(names_slice, collection_slice)

    def test_issubset(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.update((fileobj1, fileobj2))

        subset = PipelineFileCollection((fileobj1,))
        self.assertTrue(subset.issubset(self.collection))

    def test_issuperset(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.update((fileobj1,))

        superset = PipelineFileCollection((fileobj1, fileobj2))
        self.assertTrue(superset.issuperset(self.collection))

    def test_union(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3, is_deletion=True)
        self.collection.update((fileobj1, fileobj2))

        collection2 = PipelineFileCollection((fileobj3,))
        union = self.collection.union(collection2)
        self.assertSetEqual(union, PipelineFileCollection((fileobj1, fileobj2, fileobj3)))

        with self.assertRaises(TypeError):
            self.collection.union([1, 2, 3])

    def test_filter_by_attribute_id(self):
        f1 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj1.publish_type = PipelineFilePublishType.DELETE_UNHARVEST
        self.collection.add(fileobj1)

        filtered_collection = self.collection.filter_by_attribute_id('publish_type',
                                                                     PipelineFilePublishType.DELETE_UNHARVEST)
        assertCountEqual(self, self.collection, filtered_collection)

    def test_filter_by_attribute_value(self):
        f1 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        self.collection.add(fileobj1)

        filtered_collection = self.collection.filter_by_attribute_value('src_path', f1)
        assertCountEqual(self, self.collection, filtered_collection)

    def test_filter_by_attribute_regex(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        f4 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, dest_path="FOO/1", is_deletion=True)
        fileobj2 = PipelineFile(f2, dest_path="FOO/2", is_deletion=True)
        fileobj3 = PipelineFile(f3, dest_path="foo/3", is_deletion=True)
        fileobj4 = PipelineFile(f3, dest_path="BAR/1", is_deletion=True)
        self.collection.update((fileobj1, fileobj2, fileobj3, fileobj4))

        filtered_collection = self.collection.filter_by_attribute_regex('dest_path', '^FOO/[1-3]$')
        self.assertSetEqual(filtered_collection, {fileobj1, fileobj2})

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_filter_by_bool_attribute(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj1.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        self.collection.add(fileobj1)

        filtered_collection = self.collection.filter_by_bool_attribute('should_store')
        assertCountEqual(self, self.collection, filtered_collection)

        filtered_collection = self.collection.filter_by_bool_attribute('is_stored')
        self.assertSetEqual(filtered_collection, PipelineFileCollection())

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_filter_by_bool_attributes_and(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        fileobj1.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        fileobj2.publish_type = PipelineFilePublishType.DELETE_ONLY
        fileobj3.publish_type = PipelineFilePublishType.NO_ACTION

        filtered_collection1 = self.collection.filter_by_bool_attributes_and('should_harvest', 'should_store')
        self.assertSetEqual(filtered_collection1, PipelineFileCollection((fileobj1,)))

        filtered_collection2 = self.collection.filter_by_bool_attributes_and('is_deletion', 'should_store')
        self.assertSetEqual(filtered_collection2, PipelineFileCollection((fileobj2,)))

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_filter_by_bool_attributes_and_not(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        fileobj1.publish_type = PipelineFilePublishType.HARVEST_ONLY
        fileobj2.publish_type = PipelineFilePublishType.DELETE_ONLY
        fileobj3.publish_type = PipelineFilePublishType.NO_ACTION

        filtered_collection1 = self.collection.filter_by_bool_attributes_and_not(('should_harvest',), ('should_store',))
        self.assertSetEqual(filtered_collection1, PipelineFileCollection((fileobj1,)))

        filtered_collection2 = self.collection.filter_by_bool_attributes_and_not(('is_deletion',), ('should_harvest',))
        self.assertSetEqual(filtered_collection2, PipelineFileCollection((fileobj2,)))

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_filter_by_bool_attributes_not(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        fileobj1.publish_type = PipelineFilePublishType.HARVEST_ONLY
        fileobj2.publish_type = PipelineFilePublishType.DELETE_ONLY
        fileobj3.publish_type = PipelineFilePublishType.NO_ACTION

        filtered_collection = self.collection.filter_by_bool_attributes_not('should_store', 'should_harvest')

        self.assertSetEqual(filtered_collection, PipelineFileCollection((fileobj3,)))

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_filter_by_bool_attributes_or(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        fileobj1.publish_type = PipelineFilePublishType.HARVEST_ONLY
        fileobj2.publish_type = PipelineFilePublishType.DELETE_ONLY
        fileobj3.publish_type = PipelineFilePublishType.NO_ACTION

        filtered_collection = self.collection.filter_by_bool_attributes_or('should_store', 'should_harvest')

        self.assertSetEqual(filtered_collection, PipelineFileCollection((fileobj1, fileobj2)))

    def test_get_slices(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3, is_deletion=True)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        slices_of_one = self.collection.get_slices(1)
        self.assertListEqual(slices_of_one, [PipelineFileCollection(fileobj1),
                                             PipelineFileCollection(fileobj2),
                                             PipelineFileCollection(fileobj3)])

        slices_of_two = self.collection.get_slices(2)
        self.assertListEqual(slices_of_two, [PipelineFileCollection([fileobj1, fileobj2]),
                                             PipelineFileCollection(fileobj3)])

        slices_of_three = self.collection.get_slices(3)
        self.assertListEqual(slices_of_three, [PipelineFileCollection([fileobj1, fileobj2, fileobj3])])

        slices_of_four = self.collection.get_slices(4)
        self.assertListEqual(slices_of_four, [PipelineFileCollection([fileobj1, fileobj2, fileobj3])])

    def test_get_table_data(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.update((fileobj1, fileobj2))

        table_headers, table_data = self.collection.get_table_data()
        fileobj1_keys = list(OrderedDict(fileobj1).keys())
        fileobj2_keys = list(OrderedDict(fileobj2).keys())
        self.assertSequenceEqual(fileobj1_keys, table_headers)
        self.assertSequenceEqual(fileobj2_keys, table_headers)
