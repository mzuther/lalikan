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
        self._database = lalikan.database.BackupDatabase(settings, self.section)

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
            print()
        finally:
            # execute post-run command (regardless of errors)
            self.execute_command('post-run command', self.post_run_command)


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
            if len(dirs) == 0 and len(files) == 0:
                print('removing empty directory "{}"'.format(root))

                # delete directory
                os.rmdir(root)


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


    def create_backup(self, backup_type):
        return  # DEBUG

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
                self.backup_directory, reference_base, reference_catalog))
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
                self.backup_directory, reference_base, reference_catalog))

        now = datetime.datetime.now()

        timestamp = now.strftime(self._database.date_format)
        base_name = '%s-%s' % (timestamp, postfix)
        base_directory = os.path.join(self.backup_directory, base_name)
        base_file = os.path.join(base_directory, base_name)
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

        self.notify_user('%(files)d file(s), %(size)s\n' % \
                         self._get_backup_size(base_file),
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

        self.remove_empty_directories()


    def _delete_backup(self, basename):
        print('deleting backup "{}"'.format(basename))

        base_directory = os.path.join(self.backup_directory, basename)
        for backup_file in glob.glob(os.path.join(base_directory, '*.dar')):
            os.unlink(backup_file)
        for checksum_file in glob.glob(os.path.join(base_directory, '*.dar.md5')):
            os.unlink(checksum_file)


    def _get_backup_size(self, base_file):
        """
        Return number of archive files and their accumulated file size for
        an existing backup.

        :param base_file:
            path name of base file (/backup_directory/date_format/date_format)
        :type base_file:
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
