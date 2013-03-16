# -*- coding: utf-8 -*-

import configparser
import os.path
import unittest

import Lalikan.Settings


class TestSettings(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.version = '0.17'
        self.copyright_year = '2013'

        module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_filename = os.path.join(module_path, 'test.ini')
        self.settings = Lalikan.Settings.Settings(self.config_filename)
        self.section = 'Test'

        with open(self.config_filename, 'rt', encoding='utf-8') as infile:
            self.config_text = infile.read()


    def test_repr(self):
        config_text_sorted = '\n'.join(sorted(self.config_text.split('\n')))
        config_text_sorted = config_text_sorted.strip()

        self.assertEqual(
            str(self.settings),
            config_text_sorted)


    def test_get(self):
        self.assertEqual(
            self.settings.get(self.section, 'backup_client', False),
            'localhost')

        self.assertEqual(
            self.settings.get(self.section, 'backup_database', False),
            'test/test.dat')

        self.assertEqual(
            self.settings.get(self.section, 'XXXXXX_database', True),
            '')

        with self.assertRaises(configparser.NoOptionError):
            self.settings.get(self.section, 'XXXXXX_database', False)


    def test_options(self):
        options_sorted = []
        for option in sorted(self.config_text.split('\n')):
            if ':' in option:
                options_sorted.append(option[:option.find(':')])

        self.assertEqual(
            self.settings.options(self.section),
            options_sorted)


    def test_items(self):
        items_sorted = []
        for item in sorted(self.config_text.split('\n')):
            if ':' in item:
                item = tuple(item.split(': ', 1))
                items_sorted.append(item)

        self.assertEqual(
            self.settings.items(self.section),
            items_sorted)


    def test_sections(self):
        self.assertEqual(
            self.settings.sections(),
            ['Test'])


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