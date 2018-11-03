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

import datetime
import os.path
import shutil
import sys
import unittest

import lalikan.database
from lalikan.properties import BackupProperties
import lalikan.settings


# on Windows, please note that the temporary backup directory will be
# created in the root of your current working directory's drive
class TestBackupDatabase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.format = '%Y-%m-%d %H:%M:%S'

        module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_filename = os.path.join(module_path, 'test.ini')
        self.settings = lalikan.settings.Settings(self.config_filename)


    def __simulate_backups(self, database, backup_directory, faked_backups):
        # these are NOT valid backups!
        self.__simulate_backup(backup_directory, 'short', 'full',
                               False, False)
        self.__simulate_backup(backup_directory, 'pretty-long_with_1234567890',
                               'full', False, False)
        self.__simulate_backup(backup_directory, '2012-01-02_0403', 'full',
                               False, False)
        self.__simulate_backup(backup_directory, '2012-01-03_0403', 'incr',
                               True, False)
        self.__simulate_backup(backup_directory, '2012-01-04_0403', 'diff',
                               False, True)

        for timestamp, postfix in faked_backups:
            self.__simulate_backup(backup_directory, timestamp, postfix,
                                   True, True)


    def __simulate_backup(self, backup_directory, timestamp, postfix,
                          has_files, has_catalog):

        def create_file(filename):
            with open(filename, 'wt') as outfile:
                outfile.write('')


        dirname = '{timestamp}-{postfix}'.format(**locals())
        full_path = os.path.join(backup_directory, dirname)
        os.makedirs(full_path)

        if has_files:
            create_file(os.path.join(full_path, dirname + '.01.dar'))
            create_file(os.path.join(full_path, dirname + '.01.dar.md5'))
            create_file(os.path.join(full_path, dirname + '.01.dar.sha1'))
            create_file(os.path.join(full_path, dirname + '.01.dar.sha512'))

            if has_catalog:
                create_file(os.path.join(
                    full_path, timestamp + '-catalog.01.dar'))


    def test_check_backup_level(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')

        for backup_level in range(3):
            database.check_backup_level(backup_level)

        invalid_levels = [None, -1, 3, 'incr', 'differential', 'XXXX']

        for invalid_level in invalid_levels:
            with self.assertRaises(ValueError):
                database.check_backup_level(invalid_level)

        self.assertEqual(
            database.full,
            0)

        self.assertEqual(
            database.diff,
            1)

        self.assertEqual(
            database.incr,
            2)

        self.assertEqual(
            database.incr_forced,
            -1)

        check_level_names = {}

        check_level_names[None] = 'none'
        check_level_names[database.full] = 'full'
        check_level_names[database.diff] = 'differential'
        check_level_names[database.incr] = 'incremental'
        check_level_names[database.incr_forced] = 'forced incremental'

        for backup_level in check_level_names:
            self.assertEqual(
                database.get_level_name(backup_level),
                check_level_names[backup_level])


    def test_get_settings(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')

        self.assertEqual(
            database.dar_path,
            '/usr/local/bin/dar')

        self.assertEqual(
            database.dar_options,
            '--noconf --batch /etc/darrc --verbose=skipped')

        self.assertEqual(
            database.interval_full,
            9.5)

        self.assertEqual(
            database.interval_diff,
            4.0)

        self.assertEqual(
            database.interval_incr,
            1.0)

        self.assertEqual(
            database.postfix_full,
            'full')

        self.assertEqual(
            database.postfix_diff,
            'diff')

        self.assertEqual(
            database.postfix_incr,
            'incr')

        self.assertEqual(
            database.backup_directory,
            '/tmp/lalikan/test1')

        self.assertEqual(
            database.date_format,
            '%Y-%m-%d_%H%M')

        self.assertEqual(
            database.pre_run_command,
            'sudo mount -o remount,rw /mnt/backup/')

        self.assertEqual(
            database.post_run_command,
            'sudo mount -o remount,ro /mnt/backup/')


    def test_accepted_backup_levels(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')

        self.assertEqual(
            database._accepted_backup_levels(database.full),
            (database.full, ))

        self.assertEqual(
            database._accepted_backup_levels(database.diff),
            (database.full, database.diff))

        self.assertEqual(
            database._accepted_backup_levels(database.incr),
            (database.full, database.diff, database.incr))


    def __calculate_backup_schedule(self, database, current_datetime,
                                    expected_schedule):
        database.point_in_time = current_datetime
        backup_schedule = database.calculate_backup_schedule()

        result = '\n'
        for backup in backup_schedule:
            result += '{backup_suffix}:  {start_time}\n'.format(
                backup_suffix=backup.suffix,
                start_time=backup.date.strftime(self.format))

        self.assertEqual(result, expected_schedule)


    def test_calculate_backup_schedule_1(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')

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
            database.point_in_time = now

            self.assertEqual(
                database.last_scheduled_backup(database.full),
                full)

            self.assertEqual(
                database.last_scheduled_backup(database.diff),
                diff)

            self.assertEqual(
                database.last_scheduled_backup(database.incr),
                incr)


        database = lalikan.database.BackupDatabase(
            self.settings, 'Test2')
        backup_directory = database.backup_directory

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

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
                BackupProperties(None, database.full),
                BackupProperties(None, database.diff),
                BackupProperties(None, database.incr))


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
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full))


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

            self.__simulate_backup(backup_directory, '2012-01-01_2000', 'full',
                                   True, True)

            assertLastScheduledBackups(
                current_datetime,
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  4, 12, 48),
                                 database.incr))


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
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  5, 15, 12),
                                 database.diff),
                BackupProperties(datetime.datetime(2012,  1,  5, 15, 12),
                                 database.diff))


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

            self.__simulate_backup(backup_directory, '2012-01-05_1512', 'diff',
                                   True, True)

            assertLastScheduledBackups(
                current_datetime,
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  5, 15, 12),
                                 database.diff),
                BackupProperties(datetime.datetime(2012,  1,  7, 10, 24),
                                 database.incr))


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

            self.__simulate_backup(backup_directory, '2012-01-10_0800', 'incr',
                                   True, True)

            assertLastScheduledBackups(
                current_datetime,
                BackupProperties(datetime.datetime(2012,  1,  1, 20,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1,  9, 10, 24),
                                 database.diff),
                BackupProperties(datetime.datetime(2012,  1, 11,  5, 36),
                                 database.incr))


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
                BackupProperties(datetime.datetime(2012,  1, 11,  8,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1, 11,  8,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1, 11,  8,  0),
                                 database.full))


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

            self.__simulate_backup(backup_directory, '2012-01-11_0800', 'full',
                                   True, True)

            assertLastScheduledBackups(
                current_datetime,
                BackupProperties(datetime.datetime(2012,  1, 11,  8,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1, 11,  8,  0),
                                 database.full),
                BackupProperties(datetime.datetime(2012,  1, 12,  5, 36),
                                 database.incr))

        finally:
            shutil.rmtree(backup_directory)


    def test_find_existing_backups(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')
        backup_directory = database.backup_directory

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

            self.assertListEqual(
                database.find_existing_backups(),
                [])

            # valid (but faked) backups
            faked_backups = [
                BackupProperties(datetime.datetime(year=2012, month=1, day=2,
                                                   hour=2, minute=1),
                                 database.full),
                BackupProperties(datetime.datetime(year=2012, month=1, day=3,
                                                   hour=20, minute=0),
                                 database.incr),
                BackupProperties(datetime.datetime(year=2012, month=1, day=4,
                                                   hour=21, minute=34),
                                 database.incr),
                BackupProperties(datetime.datetime(year=2012, month=1, day=5,
                                                   hour=20, minute=34),
                                 database.diff),
                BackupProperties(datetime.datetime(year=2012, month=1, day=5,
                                                   hour=21, minute=34),
                                 database.incr),
            ]

            # faked directories in backup directory
            faked_directories = []
            for backup in faked_backups:
                faked_directories.append((backup.date_string, backup.suffix))

            faked_directories.append(('xxxx-xx-xx_xxxx', 'xxxx'))

            self.__simulate_backups(
                database, backup_directory, faked_directories)

            # force update of directory structure
            database.clear_cache()

            all_levels = -1

            self.assertListEqual(
                database.find_existing_backups(all_levels, datetime.datetime(
                    year=2012, month=1, day=2,
                    hour=2, minute=0)),
                [])

            self.assertListEqual(
                database.find_existing_backups(),
                faked_backups[:5])

            self.assertListEqual(
                database.find_existing_backups(
                    all_levels,
                    datetime.datetime(
                        year=2012, month=1, day=2, hour=2, minute=1)),
                faked_backups[:1])

            self.assertListEqual(
                database.find_existing_backups(
                    all_levels,
                    datetime.datetime(
                        year=2012, month=1, day=5, hour=12, minute=33)),
                faked_backups[:3])

            self.assertListEqual(
                database.find_existing_backups(
                    all_levels,
                    datetime.datetime(
                        year=2012, month=1, day=5, hour=20, minute=34)),
                faked_backups[:4])

            self.assertListEqual(
                database.find_existing_backups(
                    all_levels,
                    datetime.datetime(
                        year=2012, month=1, day=5, hour=20, minute=35)),
                faked_backups[:4])

            self.assertListEqual(
                database.find_existing_backups(
                    all_levels,
                    datetime.datetime(
                        year=2099, month=12, day=31, hour=23, minute=59)),
                faked_backups[:5])

            self.assertListEqual(
                database.find_existing_backups(
                    database.full,
                    datetime.datetime(
                        year=2099, month=12, day=31, hour=23, minute=59)),
                [faked_backups[0]])

            self.assertListEqual(
                database.find_existing_backups(
                    database.diff,
                    datetime.datetime(
                        year=2099, month=12, day=31, hour=23, minute=59)),
                [faked_backups[3]])

            self.assertListEqual(
                database.find_existing_backups(
                    database.incr,
                    datetime.datetime(
                        year=2099, month=12, day=31, hour=23, minute=59)),
                [faked_backups[1], faked_backups[2], faked_backups[4]])

        finally:
            shutil.rmtree(backup_directory)


    def test_find_last_existing_backup(self):

        def assertLastExistingBackups(now, backup_full, backup_diff,
                                      backup_incr):
            database.point_in_time = now

            self.assertEqual(
                database.last_existing_backup(database.full),
                BackupProperties(backup_full[0], backup_full[1]))

            self.assertEqual(
                database.last_existing_backup(database.diff),
                BackupProperties(backup_diff[0], backup_diff[1]))

            self.assertEqual(
                database.last_existing_backup(database.incr),
                BackupProperties(backup_incr[0], backup_incr[1]))


        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')
        backup_directory = database.backup_directory

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

            # valid (but faked) backups
            faked_backups = (
                ('2012-01-02_0201', 'full'),
                ('2012-01-03_2000', 'incr'),
                ('2012-01-04_2134', 'incr'),
                ('2012-01-05_2034', 'diff'),
                ('2012-01-05_2134', 'incr'),
            )

            self.__simulate_backups(database, backup_directory, faked_backups)

            database.point_in_time = datetime.datetime(
                year=2012, month=1, day=2,
                hour=2, minute=0)

            self.assertEqual(
                database.last_existing_backup(database.full),
                BackupProperties(None, database.full))

            self.assertEqual(
                database.last_existing_backup(database.diff),
                BackupProperties(None, database.diff))

            self.assertEqual(
                database.last_existing_backup(database.incr),
                BackupProperties(None, database.incr))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=2,
                                      hour=2, minute=1),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_incr=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=3,
                                      hour=20, minute=1),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_incr=(datetime.datetime(2012,  1,  3, 20,  0),
                             database.incr))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=6, minute=37),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_incr=(datetime.datetime(2012,  1,  4, 21, 34),
                             database.incr))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=20, minute=35),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  5, 20, 34),
                             database.diff),
                backup_incr=(datetime.datetime(2012,  1,  5, 20, 34),
                             database.diff))


            assertLastExistingBackups(
                now=datetime.datetime(year=2012, month=1, day=5,
                                      hour=22, minute=14),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  5, 20, 34),
                             database.diff),
                backup_incr=(datetime.datetime(2012,  1,  5, 21, 34),
                             database.incr))


            assertLastExistingBackups(
                now=datetime.datetime(year=2099, month=12, day=31,
                                      hour=23, minute=59),
                backup_full=(datetime.datetime(2012,  1,  2,  2,  1),
                             database.full),
                backup_diff=(datetime.datetime(2012,  1,  5, 20, 34),
                             database.diff),
                backup_incr=(datetime.datetime(2012,  1,  5, 21, 34),
                             database.incr))

        finally:
            shutil.rmtree(backup_directory)


    def test_needed_backup_level(self):

        def assertDaysOverdue(now, delta_full, delta_diff, delta_incr):
            database.point_in_time = now

            self.assertEqual(
                database.days_overdue(database.full),
                delta_full / datetime.timedelta(days=1))

            self.assertEqual(
                database.days_overdue(database.diff),
                delta_diff / datetime.timedelta(days=1))

            self.assertEqual(
                database.days_overdue(database.incr),
                delta_incr / datetime.timedelta(days=1))


        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')
        backup_directory = database.backup_directory

        no_backup_needed = None
        not_forced = False
        forced = True

        try:
            assert not os.path.exists(backup_directory)
            os.makedirs(backup_directory)

            # valid (but faked) backups
            faked_backups = (
                ('2012-01-02_2002', 'full'),
                ('2012-01-03_2000', 'incr'),
                ('2012-01-04_2134', 'incr'),
                ('2012-01-05_2034', 'diff'),
                ('2012-01-05_2134', 'incr'),
            )

            self.__simulate_backups(database, backup_directory, faked_backups)


            """
            faked_backups
            =============
            *****  2012-01-01 19:59:00
            full:  2012-01-02 20:02:00

            expected_schedule
            =================
            *****  2012-01-01 19:59:00
            full:  2012-01-01 20:00:00
            """

            # just before backup schedule starts
            now = datetime.datetime(year=2012, month=1, day=1,
                                    hour=19, minute=59)

            assertDaysOverdue(
                now=now,
                delta_full=datetime.timedelta(minutes=-1),
                delta_diff=datetime.timedelta(minutes=-1),
                delta_incr=datetime.timedelta(minutes=-1))

            # normal backup
            self.assertEqual(
                database.needed_backup_level(not_forced),
                no_backup_needed)

            # backup forced before schedule begins
            self.assertEqual(
                database.needed_backup_level(forced),
                no_backup_needed)


            """
            faked_backups
            =============
            *****  2012-01-01 20:00:00
            full:  2012-01-02 20:02:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            *****  2012-01-01 20:00:00
            incr:  2012-01-02 20:00:00
            diff:  2012-01-05 20:00:00
            full:  2012-01-11 08:00:00
            """

            # just when backup schedule starts
            now = datetime.datetime(year=2012, month=1, day=1,
                                    hour=20, minute=0)

            assertDaysOverdue(
                now=now,
                delta_full=datetime.timedelta(minutes=0),
                delta_diff=datetime.timedelta(minutes=0),
                delta_incr=datetime.timedelta(minutes=0))

            # normal backup
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.full)

            # backup forced when schedule begins
            self.assertEqual(
                database.needed_backup_level(forced),
                database.full)


            """
            faked_backups
            =============
            *****  2012-01-02 20:00:00
            full:  2012-01-02 20:02:00
            incr:  2012-01-03 20:00:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            incr:  2012-01-02 20:00:00
            *****  2012-01-02 20:00:00
            diff:  2012-01-05 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=2,
                                    hour=20, minute=1)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=1,
                                                    hour=20, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=1,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=1,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.full)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.full)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            *****  2012-01-02 20:13:00
            incr:  2012-01-03 20:00:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            incr:  2012-01-02 20:00:00
            *****  2012-01-02 20:13:00
            incr:  2012-01-03 20:00:00
            diff:  2012-01-05 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=2,
                                    hour=20, minute=13)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=5,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=3,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                no_backup_needed)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.incr_forced)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            incr:  2012-01-03 20:00:00
            *****  2012-01-03 20:01:00
            incr:  2012-01-04 21:34:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            incr:  2012-01-03 20:00:00
            *****  2012-01-03 20:01:00
            incr:  2012-01-04 20:00:00
            diff:  2012-01-05 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=3,
                                    hour=20, minute=1)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=5,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=4,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                no_backup_needed)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.incr_forced)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            incr:  2012-01-03 20:00:00
            *****  2012-01-04 20:00:00
            incr:  2012-01-04 21:34:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            incr:  2012-01-04 20:00:00
            *****  2012-01-04 20:00:00
            diff:  2012-01-05 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=4,
                                    hour=20, minute=0)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=5,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=4,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.incr)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.incr)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            incr:  2012-01-04 21:34:00
            *****  2012-01-05 20:27:00
            diff:  2012-01-05 20:34:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            diff:  2012-01-05 20:00:00
            *****  2012-01-05 20:27:00
            incr:  2012-01-06 20:00:00
            diff:  2012-01-09 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=5,
                                    hour=20, minute=27)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=5,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=5,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.diff)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.diff)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            diff:  2012-01-05 20:34:00
            *****  2012-01-05 20:35:00
            incr:  2012-01-05 21:34:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            diff:  2012-01-05 20:00:00
            *****  2012-01-05 20:35:00
            incr:  2012-01-06 20:00:00
            diff:  2012-01-09 20:00:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=5,
                                    hour=20, minute=35)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=9,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=6,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                no_backup_needed)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.incr_forced)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            diff:  2012-01-05 20:34:00
            incr:  2012-01-05 21:34:00
            *****  2012-01-10 20:01:00

            expected_schedule
            =================
            full:  2012-01-01 20:00:00
            diff:  2012-01-09 20:00:00
            incr:  2012-01-10 20:00:00
            *****  2012-01-10 20:01:00
            full:  2012-01-11 08:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=10,
                                    hour=20, minute=1)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=9,
                                                    hour=20, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=9,
                                                    hour=20, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.diff)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.diff)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            diff:  2012-01-05 20:34:00
            incr:  2012-01-05 21:34:00
            *****  2012-01-16 15:14:00

            expected_schedule
            =================
            full:  2012-01-11 08:00:00
            diff:  2012-01-15 08:00:00
            incr:  2012-01-16 08:00:00
            *****  2012-01-16 15:14:00
            incr:  2012-01-16 08:00:00
            diff:  2012-01-19 08:00:00
            full:  2012-01-20 20:00:00
            """
            now = datetime.datetime(year=2012, month=1, day=16,
                                    hour=15, minute=14)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=11,
                                                    hour=8, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.full)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.full)


            """
            faked_backups
            =============
            full:  2012-01-02 20:02:00
            diff:  2012-01-05 20:34:00
            incr:  2012-01-05 21:34:00
            full:  2012-01-12 08:01:00
            *****  2012-01-16 15:15:00

            expected_schedule
            =================
            full:  2012-01-11 08:00:00
            diff:  2012-01-15 08:00:00
            incr:  2012-01-16 08:00:00
            *****  2012-01-16 15:15:00
            incr:  2012-01-16 08:00:00
            diff:  2012-01-19 08:00:00
            full:  2012-01-20 20:00:00
            """
            self.__simulate_backup(backup_directory, '2012-01-12_0801', 'full',
                                   True, True)

            # results are memoized, so add one minute to force
            # recalculation
            now = datetime.datetime(year=2012, month=1, day=16,
                                    hour=15, minute=15)

            assertDaysOverdue(
                now=now,
                delta_full=(now - datetime.datetime(year=2012, month=1, day=20,
                                                    hour=20, minute=0)),
                delta_diff=(now - datetime.datetime(year=2012, month=1, day=15,
                                                    hour=8, minute=0)),
                delta_incr=(now - datetime.datetime(year=2012, month=1, day=15,
                                                    hour=8, minute=0)))

            # normal backup ("full" after scheduled "incr")
            self.assertEqual(
                database.needed_backup_level(not_forced),
                database.diff)

            # backup forced
            self.assertEqual(
                database.needed_backup_level(forced),
                database.diff)

        finally:
            shutil.rmtree(backup_directory)


    def test_sanitise_path(self):
        database = lalikan.database.BackupDatabase(
            self.settings, 'Test1')

        if sys.platform in ('win32', 'cygwin'):
            current_path = os.getcwd()
            os.chdir('C:\\Windows')

            self.assertEqual(
                database.sanitise_path('').lower(),
                '/cygdrive/c/windows')

            self.assertEqual(
                database.sanitise_path('.\\system32').lower(),
                '/cygdrive/c/windows/system32')

            os.chdir(current_path)
        elif sys.platform == 'linux':
            current_path = os.getcwd()

            self.assertEqual(
                database.sanitise_path(''),
                current_path)


            os.chdir('/home')

            self.assertEqual(
                database.sanitise_path(''),
                '/home')

            self.assertEqual(
                database.sanitise_path('../test/path'),
                '/test/path')


            os.chdir('/etc')

            self.assertEqual(
                database.sanitise_path(''),
                '/etc')

            self.assertEqual(
                database.sanitise_path('./test/path'),
                '/etc/test/path')

            os.chdir(current_path)
        else:
            error_message = 'Operating system "{0}" not yet supported.'
            raise EnvironmentError(error_message.format(sys.platform))


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestBackupDatabase)
