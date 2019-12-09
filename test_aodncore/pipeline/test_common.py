import uuid

from aodncore.pipeline.common import FileType
from aodncore.testlib import BaseTestCase


class TestFileType(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_get_type_from_extension_nc(self):
        nc = FileType.get_type_from_extension('.nc')
        self.assertIs(nc, FileType.NETCDF)

        nc_upper = FileType.get_type_from_extension('.NC')
        self.assertIs(nc_upper, FileType.NETCDF)

    def test_get_type_from_extension_unknown(self):
        random_extension = ".{}".format(str(uuid.uuid4()))
        unknown_type = FileType.get_type_from_extension(random_extension)
        self.assertIs(unknown_type, FileType.UNKNOWN)

    def test_get_type_from_extension_jpeg(self):
        jpg = FileType.get_type_from_extension('.jpg')
        self.assertIs(jpg, FileType.JPEG)

        jpeg = FileType.get_type_from_extension('.jpeg')
        self.assertIs(jpeg, FileType.JPEG)

        jpeg_upper = FileType.get_type_from_extension('.JPEG')
        self.assertIs(jpeg_upper, FileType.JPEG)

    def test_get_type_from_name_nc(self):
        nc_type = FileType.get_type_from_name('file.nc')
        self.assertIs(nc_type, FileType.NETCDF)

    def test_get_type_from_name_unknown(self):
        random_filename = "file.{}".format(str(uuid.uuid4()))
        unknown_type = FileType.get_type_from_name(random_filename)
        self.assertIs(unknown_type, FileType.UNKNOWN)

    def test_is_type(self):
        self.assertTrue(FileType.CSV.is_type('text'))
        self.assertTrue(FileType.NETCDF.is_type('application'))

        self.assertFalse(FileType.JPEG.is_type('text'))
        self.assertFalse(FileType.ZIP.is_type('image'))

    def test_is_image_type(self):
        self.assertTrue(FileType.JPEG.is_image_type)
        self.assertTrue(FileType.TIFF.is_image_type)

        self.assertFalse(FileType.NETCDF.is_image_type)
        self.assertFalse(FileType.UNKNOWN.is_image_type)
