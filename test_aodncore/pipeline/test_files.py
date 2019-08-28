import os
import uuid
from collections import MutableSet, OrderedDict

from six import assertCountEqual
from six.moves import range

from aodncore.pipeline.common import (CheckResult, PipelineFileCheckType, PipelineFilePublishType)
from aodncore.pipeline.exceptions import AttributeValidationError, DuplicatePipelineFileError, MissingFileError
from aodncore.pipeline.files import (PipelineFileCollection, PipelineFile, RemotePipelineFile,
                                     RemotePipelineFileCollection, ensure_pipelinefilecollection,
                                     ensure_remotepipelinefilecollection)
from aodncore.pipeline.steps import get_child_check_runner
from aodncore.testlib import BaseTestCase, NullStorageBroker, get_nonexistent_path, mock
from test_aodncore import TESTDATA_DIR

BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


class TestPipelineFiles(BaseTestCase):
    def test_ensure_pipelinefilecollection(self):
        collection_from_collection = ensure_pipelinefilecollection(PipelineFileCollection())
        self.assertIsInstance(collection_from_collection, PipelineFileCollection)

        collection_from_file = ensure_pipelinefilecollection(PipelineFile(GOOD_NC))
        self.assertIsInstance(collection_from_file, PipelineFileCollection)

        with self.assertRaises(TypeError):
            _ = ensure_pipelinefilecollection('invalid_type')

    def test_ensure_remotepipelinefilecollection(self):
        collection_from_collection = ensure_remotepipelinefilecollection(RemotePipelineFileCollection())
        self.assertIsInstance(collection_from_collection, RemotePipelineFileCollection)

        collection_from_file = ensure_remotepipelinefilecollection(RemotePipelineFile(GOOD_NC + '.dest'))
        self.assertIsInstance(collection_from_file, RemotePipelineFileCollection)

        with self.assertRaises(TypeError):
            _ = ensure_pipelinefilecollection('invalid_type')


# noinspection PyAttributeOutsideInit
class TestPipelineFile(BaseTestCase):
    def setUp(self):
        super(TestPipelineFile, self).setUp()
        deletion_path = get_nonexistent_path()
        self.pipelinefile = PipelineFile(GOOD_NC, dest_path=GOOD_NC + '.dest', name='pipelinefile')
        self.pipelinefile_deletion = PipelineFile(deletion_path, is_deletion=True)
        self.remotepipelinefile = RemotePipelineFile(GOOD_NC + '.dest', local_path=GOOD_NC, name='remotepipelinefile')

    def test_from_remotepipelinefile(self):
        expected = PipelineFile(GOOD_NC, dest_path=GOOD_NC + '.dest', name='remotepipelinefile')
        actual = PipelineFile.from_remotepipelinefile(self.remotepipelinefile)
        self.assertEqual(expected, actual)

    def test_compliance_check(self):
        # Test compliance checking
        check_runner = get_child_check_runner(PipelineFileCheckType.NC_COMPLIANCE_CHECK, None, self.test_logger,
                                              {'checks': ['cf']})
        check_runner.run(PipelineFileCollection(self.pipelinefile))
        assertCountEqual(self, dict(self.pipelinefile.check_result).keys(), ['compliant', 'errors', 'log'])

    def test_equal_files(self):
        duplicate_file = PipelineFile(GOOD_NC, name='pipelinefile')
        self.assertFalse(id(self.pipelinefile) == id(duplicate_file))
        self.assertTrue(self.pipelinefile == duplicate_file)

    def test_unequal_files(self):
        different_file = PipelineFile(BAD_NC, name='pipelinefile')
        self.assertFalse(id(self.pipelinefile) == id(different_file))
        self.assertFalse(self.pipelinefile == different_file)

    def test_format_check(self):
        # Test file format checking
        check_runner = get_child_check_runner(PipelineFileCheckType.FORMAT_CHECK, None, self.test_logger)
        check_runner.run(PipelineFileCollection(self.pipelinefile))
        assertCountEqual(self, dict(self.pipelinefile.check_result).keys(), ['compliant', 'errors', 'log'])

    def test_nonexistent_attribute(self):
        nonexistent_attribute = str(uuid.uuid4())

        with self.assertRaises(AttributeError):
            setattr(self.pipelinefile, nonexistent_attribute, None)

    def test_property_check_result(self):
        self.assertFalse(self.pipelinefile.is_checked)
        self.pipelinefile.check_result = CheckResult(True, False, None)
        self.assertTrue(self.pipelinefile.is_checked)

    def test_property_check_type(self):
        test_value = PipelineFileCheckType.FORMAT_CHECK
        self.pipelinefile.check_type = test_value
        self.assertIs(self.pipelinefile.check_type, test_value)

        with self.assertRaises(ValueError):
            self.pipelinefile.check_type = 'invalid'

        with self.assertRaises(ValueError):
            self.pipelinefile.check_type = PipelineFileCheckType.UNSET

        with self.assertRaises(ValueError):
            self.pipelinefile_deletion.check_type = PipelineFileCheckType.NONEMPTY_CHECK

    def test_property_dest_path(self):
        test_value = str(uuid.uuid4())
        self.pipelinefile.dest_path = test_value
        self.assertEqual(self.pipelinefile.dest_path, test_value)
        with self.assertRaises(ValueError):
            self.pipelinefile.dest_path = "/{uuid}".format(uuid=test_value)

    def test_property_publish_type(self):
        test_value = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.pipelinefile.publish_type = test_value
        self.assertIs(self.pipelinefile.publish_type, test_value)

        with self.assertRaises(ValueError):
            self.pipelinefile.publish_type = 'invalid'

        with self.assertRaises(ValueError):
            self.pipelinefile_deletion.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD

        with self.assertRaises(ValueError):
            self.pipelinefile.publish_type = PipelineFilePublishType.UNSET

        with self.assertRaises(ValueError):
            self.pipelinefile_deletion.publish_type = PipelineFilePublishType.UNSET

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

    def test_property_is_overwrite(self):
        self.assertFalse(self.pipelinefile.is_overwrite)
        self.pipelinefile.is_overwrite = True
        self.assertTrue(self.pipelinefile.is_overwrite)

    def test_property_mime_type(self):
        expected_default_value = 'application/octet-stream'
        expected_value = 'application/xml'

        self.assertEqual(self.pipelinefile.mime_type, expected_default_value)
        self.pipelinefile.mime_type = expected_value
        self.assertEqual(self.pipelinefile.mime_type, expected_value)

    def test_property_published_upload_only(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        self.assertEqual('No', self.pipelinefile.published)
        self.pipelinefile.is_stored = True
        self.assertEqual('Yes', self.pipelinefile.published)
        self.pipelinefile.is_upload_undone = True
        self.assertEqual('No', self.pipelinefile.published)

    def test_property_published_harvest_only(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_ONLY
        self.assertEqual('No', self.pipelinefile.published)
        self.pipelinefile.is_harvested = True
        self.assertEqual('Yes', self.pipelinefile.published)
        self.pipelinefile.is_harvest_undone = True
        self.assertEqual('No', self.pipelinefile.published)

    def test_property_published_harvest_upload(self):
        self.pipelinefile.publish_type = PipelineFilePublishType.HARVEST_UPLOAD
        self.assertEqual('No', self.pipelinefile.published)
        self.pipelinefile.is_stored = True
        self.assertEqual('No', self.pipelinefile.published)  # not harvested!
        self.pipelinefile.is_harvested = True
        self.assertEqual('Yes', self.pipelinefile.published)
        self.pipelinefile.is_upload_undone = True
        self.assertEqual('No', self.pipelinefile.published)

    def test_property_should_undo(self):
        self.assertFalse(self.pipelinefile.should_undo)
        self.pipelinefile.should_undo = True
        self.assertTrue(self.pipelinefile.should_undo)

        with self.assertRaises(ValueError):
            self.pipelinefile_deletion.should_undo = True

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


class TestRemotePipelineFile(BaseTestCase):
    def setUp(self):
        super(TestRemotePipelineFile, self).setUp()
        self.pipelinefile = PipelineFile(GOOD_NC, dest_path=GOOD_NC + '.dest', name='pipelinefile')
        self.remotepipelinefile = RemotePipelineFile(GOOD_NC + '.dest', local_path=GOOD_NC, name='remotepipelinefile')

    def test_frompipelinefile(self):
        expected = RemotePipelineFile(GOOD_NC + '.dest', local_path=GOOD_NC, name='pipelinefile')
        actual = RemotePipelineFile.from_pipelinefile(self.pipelinefile)
        self.assertEqual(expected, actual)

    def test_basename(self):
        remote_file = RemotePipelineFile('dest/path/1.nc')
        basename = os.path.basename(remote_file)
        self.assertEqual(basename, '1.nc')


# noinspection PyAttributeOutsideInit
class TestPipelineFileCollection(BaseTestCase):
    def setUp(self):
        self.collection = PipelineFileCollection()

    def tearDown(self):
        del self.collection

    def test_abstract_class(self):
        self.assertIsInstance(self.collection, MutableSet)

    def test_from_remotepipelinefilecollection(self):
        dest_path = get_nonexistent_path(relative=True)
        remote_collection = RemotePipelineFileCollection(RemotePipelineFile(dest_path, local_path=GOOD_NC,
                                                                            name='custom_name'))

        collection = PipelineFileCollection.from_remotepipelinefilecollection(remote_collection)
        expected_collection = PipelineFileCollection(PipelineFile(GOOD_NC, dest_path=dest_path, name='custom_name'))
        self.assertEqual(collection, expected_collection)

    def test_add(self):
        p1 = PipelineFile(GOOD_NC)
        p2 = PipelineFile(GOOD_NC)

        result1 = self.collection.add(p1)
        self.assertTrue(result1)

        result2 = self.collection.add(p2, overwrite=True)
        self.assertTrue(result2)

        self.assertSetEqual({p2}, self.collection)

    def test_add_duplicate(self):
        p1 = PipelineFile(GOOD_NC)
        p2 = PipelineFile(GOOD_NC)

        self.assertNotEqual(id(p1), id(p2))
        self.assertTrue(p1 == p2)

        self.collection.add(p1)
        with self.assertRaises(DuplicatePipelineFileError):
            self.collection.add(p2)

        self.assertSetEqual({p1}, self.collection)

    def test_update(self):
        p1 = PipelineFile(GOOD_NC)
        p2 = PipelineFile(BAD_NC)
        self.collection.add(p1)

        with self.assertNoException():
            self.collection.update([p2])

        self.assertSetEqual({p1, p2}, self.collection)

    def test_update_duplicate(self):
        p1 = PipelineFile(GOOD_NC)
        p2 = PipelineFile(GOOD_NC)
        self.collection.add(p1)

        with self.assertRaises(DuplicatePipelineFileError):
            self.collection.update([p2])

        self.assertIs(self.collection[0], p1)

        with self.assertNoException():
            self.collection.add(p2, overwrite=True)

        self.assertSetEqual({p2}, self.collection)

    def test_add_duplicate_dest_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        p1.dest_path = 'FIXED_DEST_PATH'
        self.collection.add(p1)

        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        p2.dest_path = 'FIXED_DEST_PATH'

        with self.assertRaises(AttributeValidationError):
            self.collection.add(p2)

    def test_add_duplicate_archive_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        p1.archive_path = 'FIXED_ARCHIVE_PATH'
        self.collection.add(p1)

        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        p2.archive_path = 'FIXED_ARCHIVE_PATH'

        with self.assertRaises(AttributeValidationError):
            self.collection.add(p2)

    def test_set_dest_paths_duplicate(self):
        def dest_path_static(src_path):
            return 'FIXED_DEST_PATH'

        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        self.collection.update((p1, p2))

        with self.assertRaises(AttributeValidationError):
            self.collection.set_dest_paths(dest_path_static)

    def test_set_archive_paths_duplicate(self):
        def archive_path_static(src_path):
            return 'FIXED_ARCHIVE_PATH'

        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        self.collection.update((p1, p2))

        with self.assertRaises(AttributeValidationError):
            self.collection.set_archive_paths(archive_path_static)

    def test_validate_attribute_value_matches_regexes(self):
        allowed_regexes = ['^VALID/PREFIX.*$']
        p1 = PipelineFile(GOOD_NC)
        p1.dest_path = 'VALID/PREFIX/TO/TEST'
        self.collection.add(p1)

        with self.assertNoException():
            self.collection.validate_attribute_value_matches_regexes('dest_path', allowed_regexes)

    def test_validate_attribute_value_matches_regexes_failure(self):
        allowed_regexes = ['^VALID/PREFIX.*$']
        p1 = PipelineFile(GOOD_NC)
        p1.dest_path = 'INVALID/PREFIX/TO/TEST'
        self.collection.add(p1)

        with self.assertRaises(AttributeValidationError):
            self.collection.validate_attribute_value_matches_regexes('dest_path', allowed_regexes)

    def test_validate_unique_attribute_value_dest_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        p1.dest_path = 'FIXED_DEST_PATH'
        self.collection.add(p1)

        with self.assertRaises(AttributeValidationError):
            self.collection.validate_unique_attribute_value('dest_path', 'FIXED_DEST_PATH')

        with self.assertNoException():
            self.collection.validate_unique_attribute_value('dest_path', 'A_DIFFERENT_DEST_PATH')

    def test_validate_unique_attribute_value_archive_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        p1.archive_path = 'FIXED_ARCHIVE_PATH'
        self.collection.add(p1)

        with self.assertRaises(AttributeValidationError):
            self.collection.validate_unique_attribute_value('archive_path', 'FIXED_ARCHIVE_PATH')

        with self.assertNoException():
            self.collection.validate_unique_attribute_value('archive_path', 'A_DIFFERENT_ARCHIVE_PATH')

    def test_validate_attribute_uniqueness_dest_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.UPLOAD_ONLY
        p1.dest_path = 'FIXED_DEST_PATH'
        self.collection.add(p1)

        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.UPLOAD_ONLY

        self.collection.add(p2)

        # edge case where the path is updated in a way that the collection cannot be aware of it
        p2.dest_path = 'FIXED_DEST_PATH'

        with self.assertRaises(AttributeValidationError):
            self.collection.validate_attribute_uniqueness('dest_path')

    def test_validate_attribute_uniqueness_archive_path(self):
        p1 = PipelineFile(GOOD_NC)
        p1.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
        p1.archive_path = 'FIXED_ARCHIVE_PATH'
        self.collection.add(p1)

        p2 = PipelineFile(BAD_NC)
        p2.publish_type = PipelineFilePublishType.ARCHIVE_ONLY

        self.collection.add(p2)

        # edge case where the path is updated in a way that the collection cannot be aware of it
        p2.archive_path = 'FIXED_ARCHIVE_PATH'

        with self.assertRaises(AttributeValidationError):
            self.collection.validate_attribute_uniqueness('archive_path')

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

        collection_names = self.collection.get_attribute_list('name')

        self.assertListEqual(names, collection_names)
        collection_names.reverse()
        with self.assertRaises(AssertionError):
            self.assertListEqual(names, collection_names)

        names_slice = names[250:750]
        collection_slice = self.collection.get_attribute_list('name')[250:750]
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

    def test_filter_by_attribute_id_not(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj1.publish_type = PipelineFilePublishType.DELETE_ONLY
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj2.publish_type = PipelineFilePublishType.DELETE_UNHARVEST
        fileobj3 = PipelineFile(f3, is_deletion=True)
        fileobj3.publish_type = PipelineFilePublishType.NO_ACTION
        self.collection.update((fileobj1, fileobj2, fileobj3))

        filtered_collection = self.collection.filter_by_attribute_id_not('publish_type',
                                                                         PipelineFilePublishType.NO_ACTION)
        assertCountEqual(self, filtered_collection, PipelineFileCollection((fileobj1, fileobj2)))

    def test_filter_by_attribute_value(self):
        f1 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        self.collection.add(fileobj1)

        filtered_collection = self.collection.filter_by_attribute_value('src_path', f1)
        assertCountEqual(self, self.collection, filtered_collection)

    def test_filter_by_attribute_regexes(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        f4 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, dest_path="FOO/1", is_deletion=True)
        fileobj2 = PipelineFile(f2, dest_path="FOO/2", is_deletion=True)
        fileobj3 = PipelineFile(f3, dest_path="foo/3", is_deletion=True)
        fileobj4 = PipelineFile(f4, dest_path="BAR/1", is_deletion=True)
        self.collection.update((fileobj1, fileobj2, fileobj3, fileobj4))

        filtered_collection = self.collection.filter_by_attribute_regexes('dest_path', '^FOO/[1-3]$')
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
    def test_filter_by_bool_attribute_not(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        filtered_collection = self.collection.filter_by_bool_attribute_not('is_deletion')
        self.assertSetEqual(filtered_collection, PipelineFileCollection((fileobj1, fileobj3)))

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

    def test_get_attribute_list(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.update((fileobj1, fileobj2))

        attribute_list = self.collection.get_attribute_list('src_path')
        self.assertListEqual(attribute_list, [f1, f2])

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

    def test_get_table_data_empty(self):
        table_headers, table_data = self.collection.get_table_data()
        self.assertListEqual([], table_headers)
        self.assertListEqual([], table_data)

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_set_bool_attribute(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        with self.assertRaises(TypeError):
            self.collection.set_bool_attribute('is_harvested', 'not_a_bool')
        with self.assertRaises(TypeError):
            self.collection.set_bool_attribute('is_harvested', 1)
        with self.assertRaises(TypeError):
            self.collection.set_bool_attribute('is_harvested', [])

        with self.assertNoException():
            self.collection.set_bool_attribute('is_harvested', True)

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_set_check_types(self, mock_isfile, mock_get_file_checksum):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2)
        self.collection.update((fileobj1, fileobj2))

        self.assertTrue(all(f.check_type is PipelineFileCheckType.UNSET for f in self.collection))
        self.collection.set_check_types(PipelineFileCheckType.NONEMPTY_CHECK)
        self.assertTrue(all(f.check_type is PipelineFileCheckType.NONEMPTY_CHECK for f in self.collection))

        with self.assertRaises(ValueError):
            self.collection.set_check_types('invalid_type')

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_set_default_check_types(self, mock_isfile, mock_get_file_checksum):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = GOOD_NC
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))
        self.collection.set_default_check_types(check_params={'checks': ['cf']})

        self.assertIs(fileobj1.check_type, PipelineFileCheckType.UNSET)
        self.assertIs(fileobj2.check_type, PipelineFileCheckType.FORMAT_CHECK)
        self.assertIs(fileobj3.check_type, PipelineFileCheckType.NC_COMPLIANCE_CHECK)

    def test_set_publish_types(self):
        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1, is_deletion=True)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        self.collection.update((fileobj1, fileobj2))

        self.assertTrue(all(f.publish_type is PipelineFilePublishType.UNSET for f in self.collection))
        self.collection.set_publish_types(PipelineFilePublishType.DELETE_UNHARVEST)
        self.assertTrue(all(f.publish_type is PipelineFilePublishType.DELETE_UNHARVEST for f in self.collection))

        with self.assertRaises(ValueError):
            self.collection.set_publish_types('invalid_type')

    @mock.patch("aodncore.pipeline.files.get_file_checksum")
    @mock.patch("os.path.isfile")
    def test_set_string_attribute(self, mock_isfile, mock_get_file_checksum):
        mock_isfile.return_value = True
        mock_get_file_checksum.return_value = ''

        f1 = get_nonexistent_path()
        f2 = get_nonexistent_path()
        f3 = get_nonexistent_path()
        fileobj1 = PipelineFile(f1)
        fileobj2 = PipelineFile(f2, is_deletion=True)
        fileobj3 = PipelineFile(f3)
        self.collection.update((fileobj1, fileobj2, fileobj3))

        with self.assertRaises(TypeError):
            self.collection.set_string_attribute('dest_path', True)
        with self.assertRaises(TypeError):
            self.collection.set_string_attribute('archive_path', 1)
        with self.assertRaises(TypeError):
            self.collection.set_string_attribute('dest_path', [])

        with self.assertNoException():
            self.collection.set_string_attribute('dest_path', 'valid/string')


class TestRemotePipelineFileCollection(BaseTestCase):
    def setUp(self):
        self.remote_collection = RemotePipelineFileCollection([
            RemotePipelineFile('dest/path/1.nc', name='1.nc'),
            RemotePipelineFile('dest/path/2.nc', name='2.nc')
        ])

    def tearDown(self):
        del self.remote_collection

    def test_from_pipelinefilecollection(self):
        dest_path = get_nonexistent_path(relative=True)
        collection = PipelineFileCollection(PipelineFile(GOOD_NC, dest_path=dest_path))
        expected_remote_collection = RemotePipelineFileCollection(
            RemotePipelineFile(dest_path, name='good.nc')
        )
        remote_collection = RemotePipelineFileCollection.from_pipelinefilecollection(collection)

        self.assertEqual(remote_collection, expected_remote_collection)

    def test_download(self):
        local_path = os.path.join(self.temp_dir, 'local_download_path')
        broker = NullStorageBroker('')

        self.remote_collection.download(broker, local_path)
        local_paths = self.remote_collection.get_attribute_list('local_path')
        expected = [os.path.join(local_path, rf.dest_path) for rf in self.remote_collection]

        broker.assert_download_call_count(1)
        self.assertItemsEqual(local_paths, expected)

    def test_file_objects(self):
        f1 = RemotePipelineFile('dest/path/1.nc', name='1.nc')
        f2 = RemotePipelineFile('dest/path/3.nc', name='3.nc')
        self.assertIn(f1, self.remote_collection)
        self.assertNotIn(f2, self.remote_collection)

        self.remote_collection.remove(f1)
        self.assertNotIn(f1, self.remote_collection)

        self.remote_collection.discard(f2)
        self.assertNotIn(f2, self.remote_collection)

        self.remote_collection.update([f1, f2])
        self.assertIn(f1, self.remote_collection)
        self.assertIn(f2, self.remote_collection)

        self.remote_collection.clear()
        assertCountEqual(self, self.remote_collection, set())

    def test_file_paths(self):
        self.assertIn('dest/path/1.nc',self.remote_collection)
        self.assertNotIn('dest/path/3.nc', self.remote_collection)

    def test_keys(self):
        actual = self.remote_collection.keys()
        expected = ['dest/path/1.nc', 'dest/path/2.nc']
        self.assertItemsEqual(actual, expected)


# noinspection PyAttributeOutsideInit
class TestRemoteFile(BaseTestCase):
    def setUp(self):
        super(TestRemoteFile, self).setUp()
        self.remotefile = RemotePipelineFile(GOOD_NC + '.dest', local_path=GOOD_NC)

    def test_equal_files(self):
        duplicate_file = RemotePipelineFile(GOOD_NC + '.dest', local_path=GOOD_NC)
        self.assertFalse(id(self.remotefile) == id(duplicate_file))
        self.assertTrue(self.remotefile == duplicate_file)

    def test_unequal_files(self):
        different_file = RemotePipelineFile(BAD_NC + '.dest', local_path=BAD_NC)
        self.assertFalse(id(self.remotefile) == id(different_file))
        self.assertFalse(self.remotefile == different_file)

    def test_nonexistent_attribute(self):
        nonexistent_attribute = str(uuid.uuid4())

        with self.assertRaises(AttributeError):
            setattr(self.remotefile, nonexistent_attribute, None)
