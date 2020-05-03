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

from vsc.utils.run import run
from vsc.utils.script_tools import CLI
from vsc.config.base import GENT_PRODUCTION_CLUSTERS

class MonthlyUsage(CLI):


    CLI_OPTIONS = {
        'users': ("File containing the list of usernames to request info for", None, "store", None),
        'cluster': ("Cluster to get information for", None, "store", GENT_PRODUCTION_CLUSTERS),
        'recipient': ("email address of the person requiring the info", None, "store", None),
    }


    def do(self, dry_run):
        """Get the information and mail it"""
        pass