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
import subprocess
import sys

import lalikan.database
import lalikan.settings


class BackupRunner:
    INFORMATION = 'information'
    WARNING = 'warning'
    ERROR = 'error'

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
        self._database = lalikan.database.BackupDatabase(
            settings, self.section)

        try:
            # execute pre-run command
            self.execute_command('pre-run command', self.pre_run_command)

            # if necessary, recursively create backup root directory
            if not os.path.exists(self.backup_directory):
                os.makedirs(self.backup_directory)

            # remove empty directories in backup root directory
            self.remove_empty_directories()

            # display time to next scheduled full backup
            print()
            print('next "full" in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(self.full),
                self._database.interval_full))

            # display time to next scheduled differential backup
            print('next "diff" in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(self.diff),
                self._database.interval_diff))

            # display time to next scheduled incremental backup
            print('next "incr" in  {:8.3f} days  ({:8.3f})'.format(
                -self._database.days_overdue(self.incr),
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
            elif backup_needed == self.incr_forced:
                backup_needed = self.incr

            # notify user that backup creation is starting
            self.notify_user('Creating {} backup...'.format(level_name),
                             self.INFORMATION)

            # create backup
            self.create_backup(backup_needed)

            # notify user that backup creation has finished
            self.notify_user('Finished.', self.INFORMATION)
            print()
        finally:
            # execute post-run command (regardless of errors)
            self.execute_command('post-run command', self.post_run_command)


    @property
    def full(self):
        """
        Attribute: internal index for full backups

        :returns:
            index for full backups
        :rtype:
            integer

        """
        return self._database.full


    @property
    def diff(self):
        """
        Attribute: internal index for differential backups

        :returns:
            index for differential backups
        :rtype:
            integer

        """
        return self._database.diff


    @property
    def incr(self):
        """
        Attribute: internal index for incremental backups

        :returns:
            index for incremental backups
        :rtype:
            integer

        """
        return self._database.incr


    @property
    def incr_forced(self):
        """
        Attribute: internal index for forced incremental backups

        :returns:
            index for forced incremental backups
        :rtype:
            integer

        """
        return self._database.incr_forced


    @property
    def backup_directory(self):
        """
        Attribute: file path for backup directory.

        :returns:
            file path for backup directory
        :rtype:
            String

        """
        return self._database.backup_directory


    @property
    def pre_run_command(self):
        """
        Attribute: command that is executed in the shell before the backup
        is started.

        :returns:
            shell command
        :rtype:
            String

        """
        return self._database.pre_run_command


    @property
    def post_run_command(self):
        """
        Attribute: command that is executed in the shell after the backup
        has finished.

        :returns:
            shell command
        :rtype:
            String

        """
        return self._database.post_run_command


    def get_base_directory(self, base_name):
        """
        Calculate base directory from base name.

        :param base_name:
            base name (e.g. "2014-12-31_1937-full")
        :type base_name:
            String

        :returns:
            base directory (e.g. "/backup_directory/base_name")
        :rtype:
            String

        """
        return os.path.join(self.backup_directory, base_name)


    def get_base_file(self, base_name):
        """
        Calculate base file from base name.

        :param base_name:
            base name (e.g. "2014-12-31_1937-full")
        :type base_name:
            String

        :returns:
            base file (e.g. "/backup_directory/base_name/base_name")
        :rtype:
            String

        """
        return os.path.join(self.get_base_directory(), base_name)


    def get_level_name(self, backup_level):
        """
        Get name for given backup level.

        :param backup_level:
            backup level (-1 to 2 or None)
        :type backup_level:
            integer

        :returns:
            name for backup level
        :rtype:
            String

        """
        return self._database.get_level_name(backup_level)


    def execute_command(self, message, command):
        """
        Execute command from shell.

        :param message:
            message to be output on the command line
        :type message:
            String

        :param command:
            complete shell command
        :type command:
            String

        :returns:
            exit code
        :rtype:
            integer

        """
        # skip on empty shell command
        if not command:
            return

        # display message on command line
        print('{}:  {}'.format(message, command))

        # run command; stdin is needed to be able to communicate with
        # the shell command (i.e. answer a question)
        proc = subprocess.Popen(
            command,
            shell=True,
            universal_newlines=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # communicate with shell command and wait for it to end
        output = proc.communicate()

        # store exit code
        retcode = proc.wait()

        # notify user of possible output on stdin
        self.notify_user(output[0], self.INFORMATION)

        # notify user of possible output on sterr
        self.notify_user(output[1], self.ERROR)

        # return exit code
        return retcode


    def execute_command_simple(self, command):
        """
        Execute command from shell.

        :param command:
            complete shell command
        :type command:
            String

        :rtype:
            None

        """
        # skip on empty shell command
        if not command:
            return

        # run command; stdin is needed to be able to communicate with
        # the shell command (i.e. answer a question)
        proc = subprocess.Popen(
            command,
            shell=True,
            universal_newlines=True,
            stdin=subprocess.PIPE)

        # communicate with shell command and wait for it to end
        proc.communicate()

        # store and return exit code
        retcode = proc.wait()
        return retcode


    def create_backup(self, backup_level):
        return  # DEBUG

        print()

        # full backup
        if backup_level == self.full:
            postfix = self._database.postfix_full
            reference_base = 'none'
            reference_option = ''
        # differential backup
        elif backup_level == self.diff:
            postfix = self._database.postfix_diff
            reference_base = self._database.last_existing_backup(
                self.full).name
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{}-{}'.format(reference_timestamp, 'catalog')
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self.backup_directory, reference_base, reference_catalog))
        # incremental backup
        elif backup_level == self.incr:
            postfix = self._database.postfix_incr
            last_full = self._database.days_since_last_backup(self.full)
            last_diff = self._database.days_since_last_backup(self.diff)
            last_incr = self._database.days_since_last_backup(self.incr)

            newest_age = last_full
            newest_type = self.full

            if last_diff >= 0 and last_diff < newest_age:
                newest_age = last_diff
                newest_type = self.diff

            if last_incr >= 0 and last_incr < newest_age:
                newest_age = last_incr
                newest_type = self.incr

            reference_base = self._database.last_existing_backup(
                newest_type).name
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{}-{}'.format(reference_timestamp, 'catalog')
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self.backup_directory, reference_base, reference_catalog))

        now = datetime.datetime.now()

        timestamp = now.strftime(self._database.date_format)
        base_name = '%s-%s' % (timestamp, postfix)

        base_directory = self.get_base_directory(base_name)
        base_file = self.get_base_file(base_name)

        catalog_name = '%s-%s' % (timestamp, "catalog")
        catalog_file = os.path.join(base_directory, catalog_name)

        print('basefile: %s\n' % base_file)

        os.mkdir(base_directory)

        command = '%(dar)s --create %(base)s %(reference)s -Q %(options)s' % \
                  {'dar': self._database.dar_path,
                   'base': self.sanitise_path(base_file),
                   'reference': reference_option,
                   'options': self._database.dar_options}

        print('creating backup: %s\n' % command)
        retcode = self.execute_command_simple(command)
        print()

        if retcode == 11:
            self.notify_user('Some files were changed during backup.',
                             self.WARNING)
        elif retcode > 0:
            # FIXME: maybe catch exceptions
            # FIXME: delete slices and directory
            self.notify_user('dar exited with code %d.' % retcode,
                             self.ERROR)

        self.notify_user('%(files)d file(s), %(size)s\n' %
                         self.get_backup_size(base_name),
                         self.INFORMATION)

        # isolate catalog
        command = '%(dar)s --isolate %(base)s --ref %(reference)s -Q %(options)s' % \
                  {'dar': self._database.dar_path,
                   'base': self.sanitise_path(catalog_file),
                   'reference': self.sanitise_path(base_file),
                   'options': self._database.dar_options}

        print('isolating catalog: %s\n' % command)
        retcode = self.execute_command_simple(command)
        print()

        if retcode == 5:
            self.notify_user('Some files do not follow chronological '
                             'order when archive index increases.',
                             self.WARNING)
        elif retcode > 0:
            # FIXME: maybe catch exceptions
            # FIXME: delete slices and directory
            raise OSError('dar exited with code %d' % retcode)

        self.delete_old_backups(backup_level)


    def find_existing_backups(self, filter_level=-1, prior_to=None):
        """
        Find existing backups.  The result can be filtered to match
        certain backup levels and to return backups prior to (or
        exactly at) a given point in time.

        :param filter_level:
            filter all backup levels other than this one (0 to 2); -1
            passes all backup levels
        :type filter_level:
            integer

        :param prior_to:
            given point in time
        :type prior_to:
            :py:mod:`datetime.datetime` or None

        :returns:
            list of existing backups, sorted by date and time
        :rtype:
            list of lalikan.properties.BackupProperties

        """
        return self._database.find_existing_backups(filter_level, prior_to)


    def delete_old_backups(self, backup_level):
        """
        Delete dispensable backups according to certain rules.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            integer

        :returns:
            None

        """
        # assert valid backup level
        self._database.check_backup_level(backup_level)

        # nothing to do for incremental backups
        if backup_level == self.incr:
            return

        # get all existing backups of backup level
        existing_backups = self.find_existing_backups(backup_level, None)

        # zero or one backups: no dispensable backups, so return
        if len(existing_backups) <= 1:
            return

        # get previous (next to last) backup of backup type
        previous_backup = existing_backups[-2]

        # get date of previous backup
        previous_date = previous_backup.date

        # full backup; please remember to never delete these!
        if backup_level == self.full:
            print()
            print('removing "diff" and "incr" prior to previous ' +
                  '"full" ({})'.format(previous_date))
            print()

            # delete differential archive files prior to previous full
            # backup
            for base_name in self.find_existing_backups(
                    self.diff, previous_date):
                self.delete_archive_files(base_name)

            # delete incremental archive files prior to previous full
            # backup
            for base_name in self.find_existing_backups(
                    self.incr, previous_date):
                self.delete_archive_files(base_name)

            # get all remaining differential backups
            existing_diffs = self.find_existing_backups(self.diff, None)

            # are any differential backups left?
            if existing_diffs:
                # get current differential backup
                current_diff = existing_diffs[-1]

                # get date of current differential backup
                current_diff_date = current_diff.date

                print()
                print('removing "incr" prior to current "diff" ({})'.format(
                    current_diff_date))
                print()

                # delete incremental archive files prior to current
                # differential backup
                for base_name in self.find_existing_backups(
                        self.incr, current_diff_date):
                    self.delete_archive_files(base_name)

        # differential backup
        elif backup_level == self.diff:
            print()
            print('removing "incr" prior to previous "diff" ({})'.format(
                previous_date))
            print()

            # delete incremental archive files prior to previous
            # differential backup
            for base_name in self.find_existing_backups(
                    self.incr, previous_date):
                self.delete_archive_files(base_name)

        # so far, only archive files have been deleted; we still have
        # to remove the remaining empty directories
        self.remove_empty_directories()


    def delete_archive_files(self, base_name):
        """
        Delete archive files in a backup directory (specified by base
        name).

        :param base_name:
            base name (e.g. "2014-12-31_1937-full")
        :type base_name:
            String

        :rtype:
            None

        """
        print('deleting backup "{}"'.format(base_name))

        # calculate base directory from base name
        base_directory = self.get_base_directory(base_name)

        # collect files that are to be deleted
        files_to_delete = []

        # mark archive files for deletion
        archive_files = glob.glob(os.path.join(base_directory, '*.dar'))
        files_to_delete.extend(archive_files)

        # mark checksum files for deletion
        checksum_files = glob.glob(os.path.join(base_directory, '*.dar.md5'))
        files_to_delete.extend(checksum_files)

        # delete marked files
        for marked_file in sorted(files_to_delete):
            os.unlink(marked_file)


    def remove_empty_directories(self):
        """
        Remove empty directories from backup filetree.

        :rtype:
            None

        """
        # loop over subdirectories of backup directory
        for root, dirs, files in os.walk(self.backup_directory, topdown=False):
            # do not remove root directory
            if root != self.backup_directory:
                continue

            # directory is empty
            if not dirs and not files:
                print('removing empty directory "{}"'.format(root))

                # delete directory
                os.rmdir(root)


    def get_backup_size(self, base_name):
        """
        Return number of archive files and their accumulated file size for
        an existing backup.

        :param base_name:
            base name (e.g. "2014-12-31_1937-full")
        :type base_name:
            String

        :returns:
            number of archive files and their accumulated file size
        :rtype:
            Tuple

        """
        # initialise number of archive files
        number_of_files = 0

        # initialise file size of archive files
        archive_size = 0

        # calculate base file from base name
        base_file = self.get_base_file(base_name)

        # get list of archive file names
        archive_files = glob.glob('{}.*.dar'.format(base_file))

        # loop over archive file names
        for archive_file in archive_files:
            # only count regular files
            if os.path.isfile(archive_file):
                # increment number of archive files
                number_of_files += 1

                # add file size to total size
                archive_size += os.stat(archive_file).st_size

        # format file size using the correct unit prefix
        if archive_size > 1e12:
            filesize = '{:.1f} TB'.format(archive_size / 1e12)
        elif archive_size > 1e9:
            filesize = '{:.1f} GB'.format(archive_size / 1e9)
        elif archive_size > 1e6:
            filesize = '{:.1f} MB'.format(archive_size / 1e6)
        elif archive_size > 1e3:
            filesize = '{:.1f} kB'.format(archive_size / 1e3)
        else:
            filesize = '{} bytes'.format(archive_size)

        # return results
        return {'files': number_of_files, 'size': filesize}


    def sanitise_path(self, path):
        """
        Return a normalised absolutised version of the specified path name.
        On Windows, the path name will also be converted to something that
        Cygwin understands (such as "/cygdrive/c/path/to/dar").

        :param path_name:
            path name
        :type path_name:
            String

        :raises:
            :py:class:`ValueError`
        :returns:
            sanitised path name
        :rtype:
            String

        """
        return self._database.sanitise_path(path)


    def notify_user(self, message, urgency):
        """
        Print message on shell and (on Linux) in notification area of the
        window manager.

        :param message:
            message to be output
        :type message:
            String

        :param urgency:
            urgency of message
        :type urgency:
            pre-defined String

        :rtype:
            None

        """
        assert urgency in (self.INFORMATION, self.WARNING, self.ERROR)

        # remove trailing white-space from message
        message = message.strip()

        # skip on empty message
        if not message:
            return

        # notify user (only works on Linux)
        if sys.platform == 'linux':
            # expire informational messages after 30 seconds
            if urgency == self.INFORMATION:
                expiration = 30
            # do not expire warnings and errors
            else:
                expiration = 0

            # compile shell command
            command = "notify-send -t {} -u {} -i {} '{}' '{}'".format(
                expiration * 1000,
                'normal',
                'dialog-{}'.format(urgency),
                'Lalikan ({})'.format(self.section),
                message)

            # run shell command
            self.execute_command_simple(command)

        # raise exception on errors
        if urgency == self.ERROR:
            raise OSError(message)
        # print warnings
        elif urgency == self.WARNING:
            print('WARNING: {}'.format(message))
        # print informational messages
        else:
            print(message)
