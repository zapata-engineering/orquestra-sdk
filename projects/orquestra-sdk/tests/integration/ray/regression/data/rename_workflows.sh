#!/usr/bin/env bash
################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

directory=$1

if [[ -z "$directory" ]]; then
  echo "usage: ./rename_workflows.sh DIRECTORY"
  exit -1
fi

if [ ! -d "$directory" ]; then
  echo "'$directory' does not exist"
  exit -1
fi

function rename_workflow {
  wf_prefix=$1
  new_suffix=$2
  for i in $(find "$directory" -iname "$1.*"); do mv "$i" "$(echo $i | sed "s/.\{7\}$/$2/g")"; done
  find "$directory" -iname "*.json" -print0 | xargs -0 sed -i '' "s/$1\..\{7\}/$1.0000001/g"
}

rename_workflow wf.multi_json_wf 0000001
rename_workflow wf.multi_pickle_wf 0000002
rename_workflow wf.single_json_wf 0000003
rename_workflow wf.single_pickle_wf 0000004
