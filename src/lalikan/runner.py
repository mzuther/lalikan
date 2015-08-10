# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2015 Dr. Martin Zuther (http://www.mzuther.de/)
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
import glob
import os
import re
import subprocess
import sys
import time

from optparse import OptionParser

import lalikan.database
import lalikan.settings


class BackupRunner:
    INFORMATION = 'information'
    WARNING = 'warning'
    ERROR = 'error'
    FINAL_ERROR = 'error'

    """
    Initialise backup runner.

    :param settings:
        backup settings and application information
    :type settings:
        lalikan.settings

    :param section:
        section of backup settings to use (such as *Workstation* or *Server*)
    :type section:
        String

    :param force_backup:
        force backup, regardless of whether it is scheduled
    :type force_backup:
        Boolean

    :rtype:
        None

    """
    def __init__(self, settings, section, force_backup):
        self.section = section
        self._database = lalikan.database.BackupDatabase(settings, self.section)

        try:
            # execute pre-run command
            self.execute_command('pre-run command', self.pre_run_command)

            # if necessary, recursively create backup root directory
            if not os.path.exists(self._database.backup_directory):
                os.makedirs(self._database.backup_directory)

            # remove empty directories in backup root directory
            self._remove_empty_directories()

            # display time to next scheduled full backup
            print()
            print('next full in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(0),
                self._database.interval_full))

            # display time to next scheduled differential backup
            print('next diff in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(1),
                self._database.interval_diff))

            # display time to next scheduled incremental backup
            print('next incr in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(2),
                self._database.interval_incr))

            # check whether a backup is necessary
            backup_needed = self._database.backup_needed(force_backup)

            # translate backup level to readable name
            # (i.e. "incremental")
            level_name = self.get_level_name(backup_needed)

            # display backup level name
            print()
            print('backup type:  ' + level_name)
            print()

            # return if no backup is scheduled
            if backup_needed is None:
                return
            # if a backup is forced, set backup type to "incremental"
            # (but do not update backup level name, so the user can
            # see that the backup has been forced)
            elif backup_needed < 0:
                backup_needed = 2

            # notify user that backup creation is starting
            self.notify_user('Creating {} backup...'.format(level_name),
                             self.INFORMATION)

            # create backup
            self.create_backup(backup_needed)

            # notify user that backup creation has finished
            self.notify_user('Finished.', self.INFORMATION)
        finally:
            # execute post-run command (regardless of errors)
            self.execute_command('post-run command', self.post_run_command)

            print('---')


    @property
    def pre_run_command(self):
        return self._database.pre_run_command


    @property
    def post_run_command(self):
        return self._database.post_run_command


    def get_level_name(self, backup_level):
        return self._database.get_level_name(backup_level)


    def _remove_empty_directories(self):
        """Remove empty directories from backup filetree

        Keyword arguments:
        None

        Return value:
        None

        """
        # initialise loop variable
        repeat = True

        while repeat:
            # will break loop until updated
            repeat = False

            # find empty directories
            for root, directories, files in os.walk(
                    self._database.backup_directory, topdown=False):
               # do not remove root directory
                if root != self._database.backup_directory:
                    # directory is empty
                    if (len(directories) < 1) and (len(files) < 1):
                        # keep looping to find *all* empty directories
                        repeat = True
                        print('removing empty directory "%(directory)s"' % \
                            {'directory': root})
                        # delete empty directory
                        os.rmdir(root)


    def execute_command(self, message, command):
        if not command:
            return

        print('{}:  {}'.format(message, command))

        # stdin is needed to be able to communicate with the
        # application (i.e. answer a question)
        proc = subprocess.Popen(
            command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        output = proc.communicate()

        # notify user of possible output on stdin
        self.notify_user(output[0], self.INFORMATION)

        # notify user of possible output on sterr
        self.notify_user(output[1], self.ERROR)


    def create_backup(self, backup_type):
        print()

        # full backup
        if backup_type == 0:
            postfix = self._database.postfix_full
            reference_base = 'none'
            reference_option = ''
        # differential backup
        elif backup_type == 1:
            postfix = self._database.postfix_diff
            reference_base = self._database.name_of_last_backup(0)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{}-{}'.format(reference_timestamp, 'catalog')
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self._database.backup_directory, reference_base,
                reference_catalog))
        # incremental backup
        elif backup_type == 2:
            postfix = self._database.postfix_incr
            last_full = self._database.days_since_last_backup(0)
            last_diff = self._database.days_since_last_backup(1)
            last_incr = self._database.days_since_last_backup(2)

            newest_age = last_full
            newest_type = 0

            if last_diff >= 0 and last_diff < newest_age:
                newest_age = last_diff
                newest_type = 1

            if last_incr >= 0 and last_incr < newest_age:
                newest_age = last_incr
                newest_type = 2

            reference_base = self._database.name_of_last_backup(newest_type)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{}-{}'.format(reference_timestamp, 'catalog')
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self._database.backup_directory, reference_base,
                reference_catalog))

        now = datetime.datetime.now()

        timestamp = now.strftime(self._database.date_format)
        base_name = '%s-%s' % (timestamp, postfix)
        base_directory = os.path.join(self._database.backup_directory, base_name)
        base_file = os.path.join(base_directory, base_name)
        catalog_name = '%s-%s' % (timestamp, "catalog")
        catalog_file = os.path.join(base_directory, catalog_name)

        print('basefile: %s\n' % base_file)

        os.mkdir(base_directory)

        cmd = '%(dar)s --create %(base)s %(reference)s -Q %(options)s' % \
              {'dar': self._database.dar_path,
               'base': self.sanitise_path(base_file),
               'reference': reference_option,
               'options': self._database.dar_options}

        print('creating backup: %s\n' % cmd)
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
        proc.communicate()
        retcode = proc.wait()
        print()

        if retcode == 11:
            self.notify_user('Some files were changed during backup.',
                             self.WARNING)
        elif retcode > 0:
            # FIXME: maybe catch exceptions
            # FIXME: delete slices and directory
            self.notify_user('dar exited with code %d.' % retcode,
                             self.ERROR)

        self.notify_user('%(files)d file(s), %(size)s\n' % \
                         self._get_backup_size(base_file),
                         self.INFORMATION)

        # isolate catalog
        cmd = '%(dar)s --isolate %(base)s --ref %(reference)s -Q %(options)s' % \
              {'dar': self._database.dar_path,
               'base': self.sanitise_path(catalog_file),
               'reference': self.sanitise_path(base_file),
               'options': self._database.dar_options}

        print('isolating catalog: %s\n' % cmd)
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
        proc.communicate()
        retcode = proc.wait()
        print()

        if retcode == 5:
            self.notify_user('Some files do not follow chronological '
                             'order when archive index increases.',
                             self.WARNING)
        elif retcode > 0:
            # FIXME: maybe catch exceptions
            # FIXME: delete slices and directory
            raise OSError('dar exited with code %d' % retcode)

        self._delete_old_backups(backup_type)


    def _delete_old_backups(self, backup_type):
        self._database.check_backup_type(backup_type)

        if backup_type == 'full':
            postfix = self._database.postfix_full
        elif backup_type == 'diff':
            postfix = self._database.postfix_diff
        elif backup_type == 'incr':
            postfix = self._database.postfix_incr

        remove_prior = self._database.find_old_backups(backup_type, None)
        if len (remove_prior) < 2:
            return
        else:
            # get date of previous backup of same type
            prior_date = remove_prior[-2]
            prior_date = prior_date[:-len(postfix) - 1]
            prior_date = datetime.datetime.strptime(
                prior_date, self._database.date_format)

        # please remember: never delete full backups!
        if backup_type == 'full':
            print('\nfull: removing diff and incr prior to last full (%s)\n' % \
                prior_date)

            for basename in self._database.find_old_backups(
                    'incr', prior_date):
                self._delete_backup(basename)
            for basename in self._database.find_old_backups(
                    'diff', prior_date):
                self._delete_backup(basename)

            # separate check for old "diff" backups
            postfix_diff = self._database.postfix_diff
            remove_prior_diff = self._database.find_old_backups(
                'diff', None)

            if (len (remove_prior) > 1) and (len(remove_prior_diff) > 0):
                # get date of last full backup
                last_full_date = remove_prior[-1]
                last_full_date = last_full_date[:-len(postfix) - 1]
                last_full_date = datetime.datetime.strptime(
                    last_full_date, self._database.date_format)

                # get date of last "diff" backup
                last_diff_date = remove_prior_diff[-1]
                last_diff_date = last_diff_date[:-len(postfix_diff) - 1]
                last_diff_date = datetime.datetime.strptime(
                    last_diff_date, self._database.date_format)

                print('\nfull: removing incr prior to last diff (%s)\n' % \
                    last_diff_date)

                for basename in self._database.find_old_backups(
                        'incr', last_diff_date):
                    self._delete_backup(basename)

        elif backup_type == 'diff':
            print('\ndiff: removing incr prior to last diff (%s)\n' % \
                prior_date)

            for basename in self._database.find_old_backups(
                    'incr', prior_date):
                self._delete_backup(basename)
        elif backup_type == 'incr':
            return

        self._remove_empty_directories()


    def _delete_backup(self, basename):
        print('deleting old backup "%s"' % basename)

        base_directory = os.path.join(self._database.backup_directory, basename)
        for backup_file in glob.glob(os.path.join(base_directory, '*.dar')):
            os.unlink(backup_file)
        for checksum_file in glob.glob(os.path.join(base_directory, '*.dar.md5')):
            os.unlink(checksum_file)


    def _get_backup_size(self, base_file):
        size = 0
        files = 0

        for single_file in glob.glob('%s.*.dar' % base_file):
            if os.path.isfile(single_file) and not os.path.islink(single_file):
                files += 1
                size += os.stat(single_file).st_size

        if size > 1e12:
            filesize = '%s TB' % round(size / 1e12, 1)
        elif size > 1e9:
            filesize = '%s GB' % round(size / 1e9, 1)
        elif size > 1e6:
            filesize = '%s MB' % round(size / 1e6, 1)
        elif size > 1e3:
            filesize = '%s kB' % round(size / 1e3, 1)
        else:
            filesize = '%s bytes' % size

        return {'files': files, 'size': filesize}


    def sanitise_path(self, path):
        return self._database.sanitise_path(path)


    def notify_user(self, message, urgency):
        if not message:
            return

        assert urgency in (self.INFORMATION, self.WARNING, self.ERROR,
                           self.FINAL_ERROR)

        if urgency == self.INFORMATION:
            # expire informational messages after 30 seconds
            expiration = 30
        else:
            # do not expire warnings and errors
            expiration = 0

        if sys.platform == 'linux':
            cmd = "notify-send -t %(expiration)d -u %(urgency)s -i %(icon)s '%(summary)s' '%(message)s'" % \
                  {'expiration': expiration * 1000,
                   'urgency': 'normal',
                   'icon': 'dialog-%s' % urgency,
                   'summary': 'Lalikan (%s)' % self.section,
                   'message': message}

            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()

        if urgency == self.ERROR:
            raise OSError(message)
        if urgency == self.FINAL_ERROR:
            print(message)
            exit(1)
        elif urgency == self.WARNING:
            print('WARNING: %s' % message)
        else:
            print('%s' % message)
