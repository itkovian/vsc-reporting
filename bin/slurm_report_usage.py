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
import yaml

from collections import namedtuple
from ConfigParser import ConfigParser
from itertools import dropwhile
from time import strftime, strptime
from copy import deepcopy

from vsc.utils.run import run
from vsc.utils.script_tools import CLI
from vsc.config.base import GENT_PRODUCTION_COMPUTE_CLUSTERS


SREPORT_TEMPLATE = "sreport --cluster={cluster} -T cpu,gres/gpu cluster UserUtilizationByAccount start={startdate} end={enddate} -t Hours --parsable2"

UsageInfo = namedtuple("UsageInfo", ["cpu", "gpu"])

import logging

class UsageReport(CLI):


    CLI_OPTIONS = {
        'userfile': ("File containing the groups of usernames to request info for (yaml format)", None, "store", None),
        'cluster': ("Cluster to get information for", None, "store", None),
        'recipient': ("email address of the person requiring the info", None, "store", None),
        'start': ("Start of time period for which to report (DD/MM/YYYY)", None, "store", None),
        'end': ("End of time period for which to report (DD/MM/YYYY)", None, "store", None),
    }

    def load_users(self):
        """Get a set of users from the given yaml file"""
        with open(self.options.userfile, "r") as userfile:
            self.users = yaml.load(userfile, Loader=yaml.Loader)  # Should be FullLoader to address CVE-2017-18342

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

        relevant_info = {}
        for (company, users) in self.users.items():
            relevant_info[company] = dict(
                filter(lambda kv: kv[0] in users, info.items())
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