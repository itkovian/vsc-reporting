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
Tools to sync with XDMoD.

- fetching data from sacct and pushing to kafka
- fetching from kafka and pushing into the shredder

@author: Andy Georges (Ghent University)
"""

# From the XDMoD guide https://open.xdmod.org/8.5/resource-manager-slurm.html
SACCT_FIELDS = [
    "jobid","jobidraw","cluster","partition","account","group","gid","user",
    "uid","submit","eligible","start","end","elapsed","exitcode","state",
    "nnodes","ncpus","reqcpus","reqmem","reqgres","reqtres","timelimit",
    "nodelist","jobname"
]

SACCT_COMMAND_TEMPLATE = [
    "TZ=UTC",
    "sacct",
    "--clusters {clusters}",
    "--allusers",
    "--parsable2",
    "--noheader",
    "--allocations",
    "--duplicates",
    "--format {fields}",
    "--state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT",
    "--starttime {startime}",
    "--endtime {endtime}",
]


SHREDDDER_COMMAND_TEMPLATE = [
    "xdmod-shredder",
    "-f slurm",
    "-d directory",
]

INGESTOR_COMMAND_TEMPLATE = [

]

class XDMoDSync(CLI):

    CLI_OPTIONS = {
        'cluster': ("Cluster to get information for. If not set, uses the Gent production cluster list.", None, "store", None),
        'brokers': ("List of kafka brokers, comma separated", str, "store", None),
        'topic': ("Kafka topic to publish to", str, "store", "xdmod"),
        'produce': ("Produce data to Kafka", bool, "store_true", False),
        'consume': ("Consume data from Kafka", bool, "store_true", False),
        'consumer_group': ("Kafka consumer group", str, "store_true", "xdmod"),
        'shredder_tmp_dir': ("Location to place the files for xdmod-shredder", str, "store", None),
        'starttime': ("Start time for jobs", str, "store", None),
        'endtime': ("End time for jobs", str, "store", None),
    }

    def do(self, dry_run):
        pass
