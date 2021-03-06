#!/bin/bash

# This file has the .sql extension, but it is actually launched as a shell
# script. This contortion is necessary because pg_regress normally uses
# psql to run the input scripts, and requires them to have the .sql
# extension, but we use a custom launcher script that runs the scripts using
# a shell instead.

TESTNAME=basictest

. sql/config_test.sh

MASTER_PG_CTL_STOP_MODE="immediate"

# Nothing to do here
function before_master
{
:
}

# Nothing to do here
function before_standby
{
:
}

# Nothing to do here
function standby_following_master
{
:
}

# Nothing to do here
function after_promotion
{
:
}

# The old master was stopped with immediate mode which will cause unclean
# shutdown and leave "in production" in the control file. At the beginning of
# pg_rewind, a single-user mode postgres session should have been run to ensure
# clean shutdown on the target instance.
function after_rewind
{
PGOPTIONS=${PGOPTIONS_UTILITY} $STANDBY_PSQL -c "SELECT state FROM pg_stat_replication"
}

# Run the test
. sql/run_test.sh
