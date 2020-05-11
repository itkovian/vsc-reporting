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
Sync data from the slurm dbd through sacct to XDMoD.

@author: Andy Georges (Ghent University)
"""

from vsc.reporting import xdmod

if __name__ == '__main__':

    sync = xdmod.XDMoDSync()
    sync.main()