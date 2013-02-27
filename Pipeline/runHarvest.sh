#!/bin/bash
ACTIVATE_PATH=../env/bin/activate
SCRIPT=PipelineHarvest.py
cd $(dirname $(readlink -f $0))
source ${ACTIVATE_PATH}
exec python ${SCRIPT} $1 $2 $3 $4 $5 $6 $7 $8 $9
