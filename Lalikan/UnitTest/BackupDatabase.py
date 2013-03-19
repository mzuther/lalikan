# -*- coding: utf-8 -*-

import datetime
import os.path
import shutil
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


    def test_check_backup_type(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)

        database._check_backup_type('full')
        database._check_backup_type('incremental')
        database._check_backup_type('differential')

        with self.assertRaises(ValueError):
            database._check_backup_type('incr')

        with self.assertRaises(ValueError):
            database._check_backup_type('diff')

        with self.assertRaises(ValueError):
            database._check_backup_type('XXXX')


    def test_get_settings(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)

        self.assertEqual(
            database.get_backup_client(),
            'localhost')

        self.assertEqual(
            database.get_backup_client_port(),
            '1234')

        self.assertEqual(
            database.get_path_to_dar(),
            '/usr/local/bin/dar')

        self.assertEqual(
            database.get_path_to_dar_manager(),
            '/usr/local/bin/dar_manager')

        self.assertEqual(
            database.get_backup_options(),
            '--noconf --batch /etc/darrc --verbose=skipped')

        self.assertEqual(
            database.get_backup_interval('full'),
            9.5)

        self.assertEqual(
            database.get_backup_interval('differential'),
            4.0)

        self.assertEqual(
            database.get_backup_interval('incremental'),
            1.0)

        self.assertEqual(
            database.get_backup_postfix('differential'),
            'diff')

        self.assertEqual(
            database.get_backup_directory(),
            '/tmp/lalikan/test1')

        self.assertEqual(
            database.get_database(),
            '/tmp/lalikan/test1/test1.dat')

        self.assertEqual(
            database.get_date_format(),
            '%Y-%m-%d_%H%M')

        self.assertEqual(
            database.get_pre_run_command(),
            'sudo mount -o remount,rw /mnt/backup/')

        self.assertEqual(
            database.get_post_run_command(),
            'sudo mount -o remount,ro /mnt/backup/')


    def __calculate_backup_schedule(self, database, current_datetime,
                                    expected_result):
        backup_schedule = database.calculate_backup_schedule(current_datetime)

        result = '\n'
        for (backup_type, backup_start_time) in backup_schedule:
            result += '{backup_type}:  {backup_start_time}\n'.format(
                backup_type=backup_type,
                backup_start_time=backup_start_time.strftime(self.format))

        self.assertEqual(result, expected_result)


    def test_calculate_backup_schedule_1(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)

        # just before first scheduled "full" backup
        current_datetime = datetime.datetime(2012, 1, 1, 19, 59)
        expected_result = """
full:  2012-01-01 20:00:00
"""
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # exactly at first scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # just before second scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # exactly at second scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # after second scheduled "full" backup
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

        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)


    def test_calculate_backup_schedule_2(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test2', self.settings)

        # just before first scheduled "full" backup
        current_datetime = datetime.datetime(2012, 1, 1, 19, 59)
        expected_result = """
full:  2012-01-01 20:00:00
"""
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # exactly at first scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # just before second scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # exactly at second scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)

        # after second scheduled "full" backup
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
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_result)


    def test_find_old_backups(self):
        def simulate_backup(timestamp, postfix, has_files, has_catalog):
            dirname = '{timestamp}-{postfix}'.format(**locals())
            full_path = os.path.join(backup_directory, dirname)
            os.makedirs(full_path)

            if has_files:
                os.mknod(os.path.join(full_path, dirname + '.01.dar'))
                os.mknod(os.path.join(full_path, dirname + '.01.dar.md5'))

                if has_catalog:
                    os.mknod(os.path.join(
                            full_path, timestamp + '-catalog.01.dar'))

        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)
        backup_directory = database.get_backup_directory()

        assert not os.path.exists(backup_directory)
        os.makedirs(backup_directory)

        try:
            self.assertEqual(
                database.find_old_backups(),
                ())

            # valid (but faked) backups
            faked_backups = (
                ('2012-01-02_0201', 'full'),
                ('2012-01-03_2000', 'incr'),
                ('2012-01-04_2134', 'incr'),
                ('2012-01-05_1234', 'diff'),
                ('2012-01-05_2134', 'incr'),
            )

            for (timestamp, postfix) in faked_backups:
                simulate_backup(timestamp, postfix, True, True)

            # these are NOT valid backups!
            simulate_backup('short', 'full', False, False)
            simulate_backup('pretty-long_with_1234567890', 'full', False, False)
            simulate_backup('2012-01-02_0403', 'full', False, False)
            simulate_backup('2012-01-03_0403', 'incr', True, False)
            simulate_backup('2012-01-04_0403', 'diff', False, True)

            self.assertEqual(
                database.find_old_backups(),
                faked_backups)

            self.assertEqual(
                database.find_old_backups(datetime.datetime(2012, 1, 5)),
                faked_backups[:3])
        finally:
            shutil.rmtree(backup_directory)


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestBackupDatabase)
