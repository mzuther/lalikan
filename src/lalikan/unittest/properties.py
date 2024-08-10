# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2024 Dr. Martin Zuther (http://www.mzuther.de/)
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

import datetime
import unittest

from lalikan.properties import BackupProperties


# on Windows, please note that the temporary backup directory will be
# created in the root of your current working directory's drive
class TestBackupProperties(unittest.TestCase):

    def setUp(self):
        self.format = '%Y-%m-%d %H:%M:%S'


    def test_correct_initialisation_1(self):
        start_time = datetime.datetime(year=2012, month=1, day=1,
                                       hour=19, minute=59)

        property = BackupProperties(start_time, 0)

        self.assertEqual(
            property.date,
            start_time)

        self.assertEqual(
            property.date_string,
            '2012-01-01_1959')

        self.assertEqual(
            property.level,
            0)

        self.assertEqual(
            property.suffix,
            'full')

        self.assertTrue(
            property.is_valid)


    def test_correct_initialisation_2(self):
        property = BackupProperties(None, 1)

        self.assertEqual(
            property.date,
            None)

        self.assertEqual(
            property.date_string,
            'None')

        self.assertEqual(
            property.level,
            1)

        self.assertEqual(
            property.suffix,
            'diff')

        self.assertFalse(
            property.is_valid)


    def test_correct_initialisation_3(self):
        property = BackupProperties(None, 2)

        self.assertEqual(
            property.level,
            2)

        self.assertEqual(
            property.suffix,
            'incr')

        self.assertFalse(
            property.is_valid)


    def test_incorrect_initialisation(self):
        start_time = datetime.datetime.now()

        with self.assertRaises(ValueError):
            BackupProperties('2012-01-01_1959', 0)

        with self.assertRaises(ValueError):
            BackupProperties(start_time, None)

        with self.assertRaises(ValueError):
            BackupProperties(start_time, 3)

        with self.assertRaises(ValueError):
            BackupProperties(start_time, 'krzm')


    def test_comparison_1(self):
        start_time_1 = datetime.datetime(year=2012, month=1, day=1,
                                         hour=20, minute=0)
        start_time_2 = start_time_1 - datetime.timedelta(minutes=1)

        property_1 = BackupProperties(start_time_1, 0)
        property_2 = BackupProperties(start_time_1, 0)
        property_3 = BackupProperties(start_time_1, 1)
        property_4 = BackupProperties(start_time_2, 0)

        self.assertTrue(
            property_1 == property_2)

        self.assertFalse(
            property_1 == property_3)

        self.assertFalse(
            property_1 == property_4)


    def test_comparison_2(self):
        start_time_1 = datetime.datetime(year=2012, month=1, day=1,
                                         hour=20, minute=0)
        start_time_2 = start_time_1 - datetime.timedelta(minutes=1)

        property_1 = BackupProperties(start_time_1, 0)
        property_2 = BackupProperties(start_time_1, 0)
        property_3 = BackupProperties(start_time_1, 1)
        property_4 = BackupProperties(start_time_2, 0)

        self.assertFalse(
            property_1 < property_2)

        self.assertFalse(
            property_1 > property_2)

        self.assertTrue(
            property_1 < property_3)

        self.assertFalse(
            property_1 > property_3)

        self.assertFalse(
            property_1 < property_4)

        self.assertTrue(
            property_1 > property_4)


    def test_sorting(self):
        start_time_1 = datetime.datetime(year=2012, month=1, day=1,
                                         hour=20, minute=0)
        start_time_2 = start_time_1 - datetime.timedelta(minutes=1)

        property_1 = BackupProperties(start_time_1, 1)
        property_2 = BackupProperties(start_time_2, 1)
        property_3 = BackupProperties(start_time_2, 0)

        unsorted = [property_1, property_2, property_3]

        self.assertEqual(
            sorted(unsorted),
            [property_3, property_2, property_1])


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestBackupProperties)
