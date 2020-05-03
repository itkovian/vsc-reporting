#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2020-2020 Ghent University
#
# This file is part of vsc-reporting,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
Get the monthly usage for the given users on the given clusters

@author: Andy Georges (Ghent University)
"""

import os
import re
import sys

from collections import namedtuple
from itertools import dropwhile
from time import strftime, strptime
from copy import deepcopy

from vsc.utils.run import run
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.config.base import GENT_PRODUCTION_COMPUTE_CLUSTERS


SREPORT_TEMPLATE = "sreport --cluster={cluster} -T cpu,gres/gpu cluster UserUtilizationByAccount start={startdate} end={enddate} -t Hours --parsable2"

UsageInfo = namedtuple("UsageInfo", ["cpu", "gpu"])

import logging
from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.generaloption import SimpleOption
from vsc.utils.lock import lock_or_bork, release_or_bork, LOCKFILE_DIR, LOCKFILE_FILENAME_TEMPLATE
from vsc.utils.nagios import (
    SimpleNagios, NAGIOS_CACHE_DIR, NAGIOS_CACHE_FILENAME_TEMPLATE, NAGIOS_EXIT_OK,
    exit_from_errorcode
)
from vsc.utils.timestamp import (
    convert_timestamp, write_timestamp, retrieve_timestamp_with_default
    )
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

DEFAULT_TIMESTAMP = "20140101000000Z"
TIMESTAMP_FILE_OPTION = 'timestamp_file'
DEFAULT_CLI_OPTIONS = {
    'start_timestamp': ("The timestamp form which to start, otherwise use the cached value", None, "store", None),
    TIMESTAMP_FILE_OPTION: ("Location to cache the start timestamp", None, "store", None),
}
MAX_DELTA = 3
MAX_RTT = 2 * MAX_DELTA + 1


def _script_name(full_name):
    """Return the script name without .py extension if any. This assumes that the script name does not contain a
    dot in case of lacking an extension.
    """
    (name, _) = os.path.splitext(full_name)
    return os.path.basename(name)


DEFAULT_OPTIONS = {
    'disable-locking': ('do NOT protect this script by a file-based lock', None, 'store_true', False),
    'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    'ha': ('high-availability master IP address', None, 'store', None),
    'locking-filename': ('file that will serve as a lock', None, 'store',
                         os.path.join(LOCKFILE_DIR,
                                      LOCKFILE_FILENAME_TEMPLATE % (_script_name(sys.argv[0]),))),
    'nagios-report': ('print out nagios information', None, 'store_true', False, 'n'),
    'nagios-check-filename': ('filename of where the nagios check data is stored', 'string', 'store',
                              os.path.join(NAGIOS_CACHE_DIR,
                                           NAGIOS_CACHE_FILENAME_TEMPLATE % (_script_name(sys.argv[0]),))),
    'nagios-check-interval-threshold': ('threshold of nagios checks timing out', 'int', 'store', 0),
    'nagios-user': ('user nagios runs as', 'string', 'store', 'nrpe'),
    'nagios-world-readable-check': ('make the nagios check data file world readable', None, 'store_true', False),
}


def _merge_options(options):
    """Merge the given set of options with the default options, updating default values where needed.

    @type options: dict. keys should be strings, values are multi-typed.
                       value is a simple scalar if the key represents an update to DEFAULT_OPTIONS\
                       value is a SimpleOption tuple otherwise
    """

    opts = deepcopy(options)
    for (k, v) in DEFAULT_OPTIONS.items():
        if k in opts:
            v_ = v[:3] + (opts[k],) + v[4:]
            opts[k] = v_
        else:
            opts[k] = v

    return opts


class CLI(object):
    """
    Base class to implement cli tools that require timestamps, nagios checks, etc.
    """
    TIMESTAMP_MANDATORY = True

    CLI_OPTIONS = {}
    CACHE_DIR = "/var/cache"

    def __init__(self, name=None, default_options=None):
        """
        Option
            name (default: script name from commandline)
            default_options: pass different set of default options
                (only when creating a new parent class; for regular child classes, use CLI_OPTIONS)
        """
        if name is None:
            name = _script_name(sys.argv[0])
        self.name = name

        self.fulloptions = self.make_options(defaults=default_options)
        self.options = self.fulloptions.options

        self.thresholds = None

        self.start_timestamp = None
        self.current_time = None

    def make_options(self, defaults=None):
        """
        Take the default sync options, set the default timestamp file and merge
        options from class constant OPTIONS

        Return ExtendedSimpleOption instance
        """
        if defaults is None:
            defaults = DEFAULT_CLI_OPTIONS
        # use a copy
        options = deepcopy(defaults)

        # insert default timestamp value file based on name
        if TIMESTAMP_FILE_OPTION in options:
            tsopt = list(options[TIMESTAMP_FILE_OPTION])
            tsopt[-1] = os.path.join(self.CACHE_DIR, "%s.timestamp" % self.name)
            options[TIMESTAMP_FILE_OPTION] = tuple(tsopt)

        options.update(self.CLI_OPTIONS)

        if TIMESTAMP_FILE_OPTION not in options and self.TIMESTAMP_MANDATORY:
            raise Exception("no mandatory %s option defined" % (TIMESTAMP_FILE_OPTION,))

        return ExtendedSimpleOption(options)

    def warning(self, msg):
        """
        Convenience method that calls ExtendedSimpleOptions warning and exists with nagios warning exitcode
        """
        exit_from_errorcode(1, msg)

    def critical(self, msg):
        """
        Convenience method that calls ExtendedSimpleOptions critical and exists with nagios critical exitcode
        """
        exit_from_errorcode(2, msg)

    def critical_exception(self, msg, exception):
        """
        Convenience method: report exception and critical method
        """
        logging.exception("%s: %s", msg, exception)
        exit_from_errorcode(2, msg)

    def do(self, dry_run):  #pylint: disable=unused-argument
        """
        Method to add actual work to do.
        The method is executed in main method in a generic try/except/finally block
        You can return something, that, when it evals to true, is considered fatal
            self.start_timestamp has start time (i.e. either passed via commandline or
                latest successful run from cache file)
            self.current_time has current_time (to be used as next start_timestamp when all goes well)
            self.options has options from commandline
            self.thresholds can be used to pass the thresholds during epilogue
        """
        logging.error("`do` method not implemented")
        raise Exception("Not implemented")

    def make_time(self):
        """
        Get start time (from commandline or cache), return current time
        """
        try:
            (start_timestamp, current_time) = retrieve_timestamp_with_default(
                getattr(self.options, TIMESTAMP_FILE_OPTION),
                start_timestamp=self.options.start_timestamp,
                default_timestamp=DEFAULT_TIMESTAMP,
                delta=-MAX_RTT,  # make the default delta explicit, current_time = now - MAX_RTT seconds
            )
        except Exception as err:
            self.critical_exception("Failed to retrieve timestamp", err)

        logging.info("Using start timestamp %s", start_timestamp)
        logging.info("Using current time %s", current_time)
        self.start_timestamp = start_timestamp
        self.current_time = current_time

    def post(self, errors, current_time=None):
        """
        Runs in main after do

        If errors evals to true, this is indicates a handled failure
        If errors evals to false, and this is not a dry_run
        it is considered success and creates the cache file with current time
        """
        if current_time is None:
            current_time = self.current_time

        if errors:
            logging.warning("Could not process all %s", errors)
            self.warning("Not all processed")
        elif not self.options.dry_run:
            # don't update the timestamp on dryrun
            timestamp = -1  # handle failing convert_timestamp
            try:
                _, timestamp = convert_timestamp(current_time)
                write_timestamp(self.options.timestamp_file, timestamp)
            except Exception as err:
                txt = "Writing timestamp %s to %s failed: %s" % (timestamp, self.options.timestamp_file, err)
                self.critical_exception(txt, err)

    def final(self):
        """
        Run as finally block in main
        """
        pass

    def main(self):
        """
        The main method.
        """
        errors = []

        msg = "Sync"
        if self.options.dry_run:
            msg += " (dry-run)"
        logging.info("%s started.", msg)

        self.make_time()

        try:
            errors = self.do(self.options.dry_run)
        except Exception as err:
            self.critical_exception("Script failed in a horrible way", err)
        finally:
            self.final()

        self.post(errors)

        self.fulloptions.epilogue("%s complete" % msg, self.thresholds)

class UsageReport(CLI):


    CLI_OPTIONS = {
        'userfile': ("File containing the list of usernames to request info for", None, "store", None),
        'cluster': ("Cluster to get information for", None, "store", None),
        'recipient': ("email address of the person requiring the info", None, "store", None),
        'start': ("Start of time period for which to report (DD/MM/YYYY)", None, "store", None),
        'end': ("End of time period for which to report (DD/MM/YYYY)", None, "store", None),
    }

    def load_users(self):
        """Get a set of users from the given file"""
        with open(self.options.userfile, "r") as userfile:
            self.users = set([l.rstrip() for l in userfile.readlines() if l.startswith("vsc")])

        logging.debug("Checking for users: %s", self.users)

    def convert_date(self, date):
        """Convert date to the required MM/DD/YYYY format"""
        return strftime("%M/%d/%y", strptime(date, "%d/%M/%Y"))

    def process(self, output):
        """Convert the output of the sreport command into a list."""
        lines = output.splitlines()

        # The useful data occurs after the second line of dashes
        sep = re.compile(r'^--*$')
        drop = lambda ls, f: list(dropwhile(lambda l: not(f(l)), ls))[1:]
        lines = drop(lines, lambda l: sep.match(l))
        lines = drop(lines, lambda l: sep.match(l))

        # The first line now contains the column headers
        headers = lines[0].split('|')
        lines = lines[1:]

        user_info = {}
        # we have two xRES lines per user, so we should process them together
        for i in range(0, len(lines)/2):
            cpu_line = lines[2*i].split('|')
            gpu_line = lines[2*i+1].split('|')

            user = cpu_line[1]  # login
            cpu_usage = int(cpu_line[-1])
            gpu_usage = int(gpu_line[-1])

            user_info[user] = UsageInfo(cpu=cpu_usage, gpu=gpu_usage)
            logging.debug("Adding user %s info %d %d", user, cpu_usage, gpu_usage)

        return user_info

    def report(self, cluster):
        """Make a report for the given cluster"""

        logging.debug("Start date: %s", self.convert_date(self.options.start))
        logging.debug("End date: %s", self.convert_date(self.options.end))

        sreport_command = SREPORT_TEMPLATE.format(
            cluster=cluster,
            startdate=self.convert_date(self.options.start),
            enddate=self.convert_date(self.options.end)
        )

        ec, output = run(sreport_command)

        logging.info("Report command ran, ec = %d", ec)

        info = self.process(output)

        relevant_info = dict(
            filter(lambda kv: kv[0] in self.users, info.items())
        )

        logging.debug("Desired user info: %s", relevant_info)

    def do(self, dry_run):
        """Get the information and mail it"""

        clusters = GENT_PRODUCTION_COMPUTE_CLUSTERS
        if self.options.cluster:
            clusters = (self.options.cluster,)

        self.load_users()

        for cluster in clusters:
            self.report(cluster)


if __name__ == '__main__':

    rep = UsageReport()
    rep.main()