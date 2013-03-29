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
                                    expected_schedule):
        backup_schedule = database.calculate_backup_schedule(current_datetime)

        result = '\n'
        for (backup_start_time, backup_type) in backup_schedule:
            result += '{backup_type}:  {backup_start_time}\n'.format(
                backup_type=backup_type,
                backup_start_time=backup_start_time.strftime(self.format))

        self.assertEqual(result, expected_schedule)


    def test_calculate_backup_schedule_1(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)

        # just before first scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=1,
                                             hour=19, minute=59)
        expected_schedule = """
full:  2012-01-01 20:00:00
"""
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_schedule)

        # exactly at first scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=1,
                                             hour=20, minute=0)
        expected_schedule = """
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
                                         expected_schedule)

        # just before second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=11,
                                             hour=7, minute=59)
        expected_schedule = """
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
                                         expected_schedule)

        # exactly at second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=11,
                                             hour=8, minute=0)
        expected_schedule = """
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
                                         expected_schedule)

        # after second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=12,
                                             hour=11, minute=59)
        expected_schedule = """
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
                                         expected_schedule)


    def test_calculate_backup_schedule_2(self):

        def assertLastScheduledBackups(now, full, diff, incr):
            self.assertEqual(
                database.last_scheduled_backup('full', now),
                full)

            self.assertEqual(
                database.last_scheduled_backup('differential', now),
                diff)

            self.assertEqual(
                database.last_scheduled_backup('incremental', now),
                incr)


        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test2', self.settings)


        # just before first scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=1,
                                             hour=19, minute=59)
        expected_schedule = """
full:  2012-01-01 20:00:00
"""
        self.__calculate_backup_schedule(database, current_datetime,
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            None,
            None,
            None)


        # exactly at first scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=1,
                                             hour=20, minute=0)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'))


        # before first "diff" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=5,
                                             hour=10, minute=0)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  4, 12, 48), 'incr'))


        # after first "diff" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=5,
                                             hour=16, minute=2)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  5, 15, 12), 'diff'),
            (datetime.datetime(2012,  1,  5, 15, 12), 'diff'))


        # two days later ...
        current_datetime = datetime.datetime(year=2012, month=1, day=7,
                                             hour=11, minute=12)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  5, 15, 12), 'diff'),
            (datetime.datetime(2012,  1,  7, 10, 24), 'incr'))


        # just before second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=11,
                                             hour=7, minute=59)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1,  1, 20,  0), 'full'),
            (datetime.datetime(2012,  1,  9, 10, 24), 'diff'),
            (datetime.datetime(2012,  1, 11,  5, 36), 'incr'))


        # exactly at second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=11,
                                             hour=8, minute=0)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1, 11,  8,  0), 'full'),
            (datetime.datetime(2012,  1, 11,  8,  0), 'full'),
            (datetime.datetime(2012,  1, 11,  8,  0), 'full'))


        # after second scheduled "full" backup
        current_datetime = datetime.datetime(year=2012, month=1, day=12,
                                             hour=11, minute=59)
        expected_schedule = """
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
                                         expected_schedule)

        assertLastScheduledBackups(
            current_datetime,
            (datetime.datetime(2012,  1, 11,  8,  0), 'full'),
            (datetime.datetime(2012,  1, 11,  8,  0), 'full'),
            (datetime.datetime(2012,  1, 12,  5, 36), 'incr'))


    def simulate_backups(self, database, backup_directory):
        # these are NOT valid backups!
        self.simulate_backup(backup_directory, 'short', 'full',
                             False, False)
        self.simulate_backup(backup_directory, 'pretty-long_with_1234567890',
                             'full', False, False)
        self.simulate_backup(backup_directory, '2012-01-02_0403', 'full',
                             False, False)
        self.simulate_backup(backup_directory, '2012-01-03_0403', 'incr',
                             True, False)
        self.simulate_backup(backup_directory, '2012-01-04_0403', 'diff',
                             False, True)

        # valid (but faked) backups
        self.faked_backups = (
            ('2012-01-02_0201', 'full'),
            ('2012-01-03_2000', 'incr'),
            ('2012-01-04_2134', 'incr'),
            ('2012-01-05_1234', 'diff'),
            ('2012-01-05_2134', 'incr'),
        )

        for (timestamp, postfix) in self.faked_backups:
            self.simulate_backup(backup_directory, timestamp, postfix,
                                 True, True)


    def simulate_backup(self, backup_directory, timestamp, postfix,
                        has_files, has_catalog):
        dirname = '{timestamp}-{postfix}'.format(**locals())
        full_path = os.path.join(backup_directory, dirname)
        os.makedirs(full_path)

        if has_files:
            os.mknod(os.path.join(full_path, dirname + '.01.dar'))
            os.mknod(os.path.join(full_path, dirname + '.01.dar.md5'))

            if has_catalog:
                os.mknod(os.path.join(
                        full_path, timestamp + '-catalog.01.dar'))


    def test_find_old_backups(self):
        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)
        backup_directory = database.get_backup_directory()

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

            self.assertTupleEqual(
                database.find_old_backups(),
                ())

            self.simulate_backups(database, backup_directory)

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2012, month=1, day=2,
                        hour=2, minute=0)),
                ())

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2012, month=1, day=2,
                        hour=2, minute=1)),
                self.faked_backups[:1])

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2012, month=1, day=5,
                        hour=12, minute=33)),
                self.faked_backups[:3])

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2012, month=1, day=5,
                        hour=12, minute=34)),
                self.faked_backups[:4])

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2012, month=1, day=5,
                        hour=12, minute=35)),
                self.faked_backups[:4])

            self.assertTupleEqual(
                database.find_old_backups(datetime.datetime(
                        year=2099, month=12, day=31,
                        hour=23, minute=59)),
                self.faked_backups)

            self.assertTupleEqual(
                database.find_old_backups(),
                self.faked_backups)

        finally:
            shutil.rmtree(backup_directory)


    def test_find_last_existing_backup(self):

        def assertLastExistingBackups(now, backup_full, backup_diff,
                                      backup_incr):
            self.assertTupleEqual(
                database.last_existing_backup('full', now),
                backup_full)

            self.assertTupleEqual(
                database.last_existing_backup('differential', now),
                backup_diff)

            self.assertTupleEqual(
                database.last_existing_backup('incremental', now),
                backup_incr)


        database = Lalikan.BackupDatabase.BackupDatabase(
            'Test1', self.settings)
        backup_directory = database.get_backup_directory()

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

            self.simulate_backups(database, backup_directory)

            now = datetime.datetime(year=2012, month=1, day=2,
                                    hour=2, minute=0)

            self.assertEqual(
                database.last_existing_backup('full', now),
                None)

            self.assertEqual(
                database.last_existing_backup('differential', now),
                None)

            self.assertEqual(
                database.last_existing_backup('incremental', now),
                None)


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=2,
                                      hour=2, minute=1),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_incr=(datetime.datetime(2012,  1,  2,  2,  1), 'full'))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=3,
                                      hour=20, minute=1),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_incr=(datetime.datetime(2012,  1,  3, 20,  0), 'incr'))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=6, minute=37),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_incr=(datetime.datetime(2012,  1,  4, 21, 34), 'incr'))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=12, minute=35),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  5, 12, 34), 'diff'),
                backup_incr=(datetime.datetime(2012,  1,  5, 12, 34), 'diff'))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=22, minute=14),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  5, 12, 34), 'diff'),
                backup_incr=(datetime.datetime(2012,  1,  5, 21, 34), 'incr'))


            assertLastExistingBackups(
                now=datetime.datetime(year=2099, month=12, day=31,
                                      hour=23, minute=59),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1), 'full'),
                backup_diff=(datetime.datetime(2012,  1,  5, 12, 34), 'diff'),
                backup_incr=(datetime.datetime(2012,  1,  5, 21, 34), 'incr'))
        finally:
            shutil.rmtree(backup_directory)


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestBackupDatabase)
