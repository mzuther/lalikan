# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2018 Dr. Martin Zuther (http://www.mzuther.de/)
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

        module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_filename = os.path.join(module_path, 'test.json')
        self.settings = lalikan.settings.Settings(self.config_filename)
        self.section = 'Test1'

        with open(self.config_filename, 'rt', encoding='utf-8') as infile:
            self.config_text = infile.read()


    def test_get(self):
        self.assertEqual(
            self.settings.get(self.section, 'backup-directory', False),
            '/tmp/lalikan/test1')

        self.assertEqual(
            self.settings.get(self.section, 'XXXXXX-directory', True),
            '')

        with self.assertRaises(KeyError):
            self.settings.get(self.section, 'XXXXXX-directory', False)

        with self.assertRaises(KeyError):
            self.settings.get('no_section', 'backup-directory', False)


    def test_options(self):
        options = [
            "backup-directory",
            "command-notification",
            "command-post-run",
            "command-pre-run",
            "dar-options",
            "dar-path",
            "interval-diff",
            "interval-full",
            "interval-incr",
            "start-time"
        ]

        self.assertTupleEqual(
            self.settings.options(self.section),
            tuple(options))


    def test_items(self):
        items = (
            ("backup-directory", "/tmp/lalikan/test1"),
            ("command-notification", "notify-send -t {expiration} -u normal -i dialog-{urgency} '{application}' '{message}'"),
            ("command-post-run", "sudo mount -o remount,ro /mnt/backup/"),
            ("command-pre-run", "sudo mount -o remount,rw /mnt/backup/"),
            ("dar-options", "--noconf --batch /etc/darrc --verbose=skipped"),
            ("dar-path", "/usr/local/bin/dar"),
            ("interval-diff", 4.0),
            ("interval-full", 9.5),
            ("interval-incr", 1.0),
            ("start-time", "2012-01-01_2000")
        )

        self.assertTupleEqual(
            self.settings.items(self.section),
            tuple(items))


    def test_sections(self):
        self.assertEqual(
            self.settings.sections(),
            ('Default', 'aaa', 'Test1', 'Test2', 'zzz'))


    def test_get_option(self):
        self.assertEqual(
            self.settings.get_option('application'),
            'Lalikan.py')

        with self.assertRaises(ValueError):
            self.settings.get_option('XXXlication')


    def test_get_description(self):
        self.assertEqual(
            self.settings.get_description(),
            'Backup scheduler for Disk ARchive (DAR).')


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSettings)
