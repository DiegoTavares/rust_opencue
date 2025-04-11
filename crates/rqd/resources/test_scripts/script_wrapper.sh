#!/bin/bash

source "$1"
exit_code=$?
echo $exit_code > $2
exit $exit_code
