# -*- coding: utf-8 -*-

import datetime
import os.path
import unittest

import Lalikan.BackupDatabase
import Lalikan.Settings


class TestBackupDatabase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.format = '%Y-%m-%d %H:%M:%S'

        module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_filename = os.path.join(module_path, 'test.ini')
        self.settings = Lalikan.Settings.Settings(self.config_filename)


    def __calculate_backup_schedule(self, section, current_datetime,
                                    expected_result):
        database = Lalikan.BackupDatabase.BackupDatabase(
            section, self.settings)

        backup_schedule = database.calculate_backup_schedule(current_datetime)

        result = '\n'
        for (backup_type, backup_start_time) in backup_schedule:
            result += '{backup_type}:  {backup_start_time}\n'.format(
                backup_type=backup_type,
                backup_start_time=backup_start_time.strftime(self.format))

        self.assertEqual(result, expected_result)


    def test_calculate_backup_schedule_1_1(self):
        # just before first scheduled "full" backup
        section = 'Test1'
        current_datetime = datetime.datetime(2012, 1, 1, 19, 59)

        expected_result = """
full:  2012-01-01 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_1_2(self):
        # exactly at first scheduled "full" backup
        section = 'Test1'
        current_datetime = datetime.datetime(2012, 1, 1, 20, 0)

        expected_result = """
full:  2012-01-01 20:00:00
incr:  2012-01-02 20:00:00
incr:  2012-01-03 20:00:00
incr:  2012-01-04 20:00:00
diff:  2012-01-05 20:00:00
incr:  2012-01-06 20:00:00
incr:  2012-01-07 20:00:00
incr:  2012-01-08 20:00:00
diff:  2012-01-09 20:00:00
incr:  2012-01-10 20:00:00
full:  2012-01-11 08:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_1_3(self):
        # just before second scheduled "full" backup
        section = 'Test1'
        current_datetime = datetime.datetime(2012, 1, 11, 7, 59)

        expected_result = """
full:  2012-01-01 20:00:00
incr:  2012-01-02 20:00:00
incr:  2012-01-03 20:00:00
incr:  2012-01-04 20:00:00
diff:  2012-01-05 20:00:00
incr:  2012-01-06 20:00:00
incr:  2012-01-07 20:00:00
incr:  2012-01-08 20:00:00
diff:  2012-01-09 20:00:00
incr:  2012-01-10 20:00:00
full:  2012-01-11 08:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_1_4(self):
        # exactly at second scheduled "full" backup
        section = 'Test1'
        current_datetime = datetime.datetime(2012, 1, 11, 8, 0)

        expected_result = """
full:  2012-01-11 08:00:00
incr:  2012-01-12 08:00:00
incr:  2012-01-13 08:00:00
incr:  2012-01-14 08:00:00
diff:  2012-01-15 08:00:00
incr:  2012-01-16 08:00:00
incr:  2012-01-17 08:00:00
incr:  2012-01-18 08:00:00
diff:  2012-01-19 08:00:00
incr:  2012-01-20 08:00:00
full:  2012-01-20 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_1_5(self):
        # after second scheduled "full" backup
        section = 'Test1'
        current_datetime = datetime.datetime(2012, 1, 12, 11, 59)

        expected_result = """
full:  2012-01-11 08:00:00
incr:  2012-01-12 08:00:00
incr:  2012-01-13 08:00:00
incr:  2012-01-14 08:00:00
diff:  2012-01-15 08:00:00
incr:  2012-01-16 08:00:00
incr:  2012-01-17 08:00:00
incr:  2012-01-18 08:00:00
diff:  2012-01-19 08:00:00
incr:  2012-01-20 08:00:00
full:  2012-01-20 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2_1(self):
        # just before first scheduled "full" backup
        section = 'Test2'
        current_datetime = datetime.datetime(2012, 1, 1, 19, 59)

        expected_result = """
full:  2012-01-01 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2_2(self):
        # exactly at first scheduled "full" backup
        section = 'Test2'
        current_datetime = datetime.datetime(2012, 1, 1, 20, 0)

        expected_result = """
full:  2012-01-01 20:00:00
incr:  2012-01-02 17:36:00
incr:  2012-01-03 15:12:00
incr:  2012-01-04 12:48:00
incr:  2012-01-05 10:24:00
diff:  2012-01-05 15:12:00
incr:  2012-01-06 12:48:00
incr:  2012-01-07 10:24:00
incr:  2012-01-08 08:00:00
incr:  2012-01-09 05:36:00
diff:  2012-01-09 10:24:00
incr:  2012-01-10 08:00:00
incr:  2012-01-11 05:36:00
full:  2012-01-11 08:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2_3(self):
        # just before second scheduled "full" backup
        section = 'Test2'
        current_datetime = datetime.datetime(2012, 1, 11, 7, 59)

        expected_result = """
full:  2012-01-01 20:00:00
incr:  2012-01-02 17:36:00
incr:  2012-01-03 15:12:00
incr:  2012-01-04 12:48:00
incr:  2012-01-05 10:24:00
diff:  2012-01-05 15:12:00
incr:  2012-01-06 12:48:00
incr:  2012-01-07 10:24:00
incr:  2012-01-08 08:00:00
incr:  2012-01-09 05:36:00
diff:  2012-01-09 10:24:00
incr:  2012-01-10 08:00:00
incr:  2012-01-11 05:36:00
full:  2012-01-11 08:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2_4(self):
        # exactly at second scheduled "full" backup
        section = 'Test2'
        current_datetime = datetime.datetime(2012, 1, 11, 8, 0)

        expected_result = """
full:  2012-01-11 08:00:00
incr:  2012-01-12 05:36:00
incr:  2012-01-13 03:12:00
incr:  2012-01-14 00:48:00
incr:  2012-01-14 22:24:00
diff:  2012-01-15 03:12:00
incr:  2012-01-16 00:48:00
incr:  2012-01-16 22:24:00
incr:  2012-01-17 20:00:00
incr:  2012-01-18 17:36:00
diff:  2012-01-18 22:24:00
incr:  2012-01-19 20:00:00
incr:  2012-01-20 17:36:00
full:  2012-01-20 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2_5(self):
        # after second scheduled "full" backup
        section = 'Test2'
        current_datetime = datetime.datetime(2012, 1, 12, 11, 59)

        expected_result = """
full:  2012-01-11 08:00:00
incr:  2012-01-12 05:36:00
incr:  2012-01-13 03:12:00
incr:  2012-01-14 00:48:00
incr:  2012-01-14 22:24:00
diff:  2012-01-15 03:12:00
incr:  2012-01-16 00:48:00
incr:  2012-01-16 22:24:00
incr:  2012-01-17 20:00:00
incr:  2012-01-18 17:36:00
diff:  2012-01-18 22:24:00
incr:  2012-01-19 20:00:00
incr:  2012-01-20 17:36:00
full:  2012-01-20 20:00:00
"""

        self.__calculate_backup_schedule(section, current_datetime,
                                         expected_result)


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestBackupDatabase)
