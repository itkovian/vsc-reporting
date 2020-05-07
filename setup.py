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
vsc-reporting base distribution setup.py

@author: Andy Georges (Ghent University)
"""
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag

PACKAGE = {
    'version': '0.1.1',
    'author': [ag],
    'maintainer': [ag],
    'setup_requires': ['vsc-install >= 0.15.1'],
    'install_requires': [
        'vsc-base >= 3.0.1',
        'vsc-utils >= 2.1.2',
        'future >= 0.16.0',
        'python2-pyyaml >= 3.10',
    ],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
