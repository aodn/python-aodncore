import os

from aodncore.testlib import BaseTestCase
from aodncore.vocab.platform_code_vocab import (PlatformVocabHelper, platform_type_uris_by_category,
                                                platform_altlabels_per_preflabel)

TEST_ROOT = os.path.join(os.path.dirname(__file__))

PLATFORM_CAT_VOCAB_URL = '%s%s' % ('file://',
                                   os.path.join(TEST_ROOT, 'aodn_aodn-platform-category-vocabulary.rdf'))

DEFAULT_PLATFORM_VOCAB_URL = '%s%s' % ('file://',
                                       os.path.join(TEST_ROOT, 'aodn_aodn-platform-vocabulary.rdf'))


class TestPlatformCodeVocab(BaseTestCase):
    def setUp(self):
        self.platform_vocab_helper = PlatformVocabHelper.from_config(self.config)

    def test_platform_type(self):
        res = self.platform_vocab_helper.platform_type_uris_by_category()
        self.assertEqual(res['Float'][0], 'http://vocab.nerc.ac.uk/collection/L06/current/42')

    def test_altlabels(self):
        res = self.platform_vocab_helper.platform_altlabels_per_preflabel(category_name='Vessel')
        self.assertEqual(res['9VUU'], 'Anro Asia')

    def test_platform_type_deprecated(self):
        res = platform_type_uris_by_category(platform_cat_vocab_url=PLATFORM_CAT_VOCAB_URL)
        self.assertEqual(res['Float'][0], 'http://vocab.nerc.ac.uk/collection/L06/current/42')

    def test_altlabels_deprecated(self):
        res = platform_altlabels_per_preflabel(category_name='Vessel', platform_vocab_url=DEFAULT_PLATFORM_VOCAB_URL)
        self.assertEqual(res['9VUU'], 'Anro Asia')
