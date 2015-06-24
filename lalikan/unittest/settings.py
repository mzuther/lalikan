# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2015 Martin Zuther (http://www.mzuther.de/)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Thank you for using free software!

import configparser
import os.path
import unittest

import lalikan.settings


class TestSettings(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.version = '0.17'
        self.copyright_year = '2015'

        module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_filename = os.path.join(module_path, 'test.ini')
        self.settings = lalikan.settings.Settings(self.config_filename)
        self.section = 'Test1'

        with open(self.config_filename, 'rt', encoding='utf-8') as infile:
            self.config_text = infile.read()


    def test_get(self):
        self.assertEqual(
            self.settings.get(self.section, 'backup_directory', False),
            '/tmp/lalikan/test1')

        self.assertEqual(
            self.settings.get(self.section, 'XXXXXX_directory', True),
            '')

        with self.assertRaises(configparser.NoOptionError):
            self.settings.get(self.section, 'XXXXXX_directory', False)

        with self.assertRaises(configparser.NoSectionError):
            self.settings.get('no_section', 'backup_directory', False)


    def test_options(self):
        options = []
        for option in self.config_text.split('\n'):
            if option == '[Test2]':
                break
            elif ':' in option:
                options.append(option[:option.find(':')])


        self.assertTupleEqual(
            self.settings.options(self.section),
            tuple(sorted(options, key=str.lower)))


    def test_items(self):
        items = []
        for item in self.config_text.split('\n'):
            if item == '[Test2]':
                break
            elif ':' in item:
                item = tuple(item.split(': ', 1))
                items.append(item)

        self.assertTupleEqual(
            self.settings.items(self.section),
            tuple(sorted(items, key=lambda i: str.lower(i[0]))))


    def test_sections(self):
        self.assertEqual(
            self.settings.sections(),
            ('Default', 'aaa', 'Test1', 'Test2', 'zzz'))


    def test_get_option(self):
        self.assertEqual(
            self.settings.get_option('application'),
            'Lalikan.py')

        self.assertEqual(
            self.settings.get_option('version'),
            self.version)

        self.assertEqual(
            self.settings.get_option('authors'),
            'Martin Zuther')

        with self.assertRaises(ValueError):
            self.settings.get_option('XXXlication')


    def test_get_description(self):
        self.assertEqual(
            self.settings.get_description(False),
            'Lalikan.py v{0}\n================'.format(self.version))

        self.assertEqual(
            self.settings.get_description(True),
            'Lalikan.py v{0}\n================\nBackup scheduler for Disk ARchive (DAR)'.format(self.version))


    def test_get_copyrights(self):
        self.assertEqual(
            self.settings.get_copyrights(),
            '(c) 2010-{0} Martin Zuther'.format(self.copyright_year))


    def test_get_license(self):
        self.assertEqual(
            self.settings.get_license(False),
            'GPL version 3 (or later)')

        self.assertEqual(
            self.settings.get_license(True),
            """This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Thank you for using free software!""")


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSettings)
