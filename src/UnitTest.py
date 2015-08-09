#!/usr/bin/env python3

"""Lalikan
   =======
   Backup scheduler for Disk ARchive (DAR)

   Copyright (c) 2010-2015 Dr. Martin Zuther (http://www.mzuther.de/)

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Thank you for using free software!

"""

import importlib
import sys
import unittest

# available unit tests
valid_tests = [
    'lalikan.unittest.database',
    'lalikan.unittest.properties',
    'lalikan.unittest.settings',
]

# sort available unit tests by name
valid_tests = sorted(valid_tests)

# get specified unit tests (if any) from command line
specified_tests = sorted(sys.argv[1:])

# some unit tests were specified on the command line
if specified_tests:
    # loop over specified unit tests
    for test_suite in specified_tests:
        # check if unit test exists
        if test_suite not in valid_tests:
            print('')
            print('    No test suite found in module "{}".'.format(test_suite))
            print('')
            print('    Valid test suites are:')
            print('')

            # list available unit tests
            for test_class in valid_tests:
                print('    * ' + test_class)
            print('')

            # signal error
            exit(1)
# otherwise, run the whole lot
else:
    specified_tests = valid_tests

# initialise test runner
verbosity = 0
test_runner = unittest.TextTestRunner(verbosity=verbosity)

# loop over specified unit tests
for test_suite in specified_tests:
    # output name of unit test class
    print('')
    print(test_suite)

    # import unit test
    module = importlib.import_module(test_suite)

    # run unit test
    test_runner.run(module.get_suite())

print('')
