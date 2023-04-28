# Ray Regression Data

Each subdirectory in this directory is a Ray Storage directory.

This directory's location is controlled by:

```
export ORQ_RAY_STORAGE_PATH=...
```

## Building New Workflow Storage

Create the new directory:

```
mkdir ./0.99.0
```

Set the Ray Storage path:

```
export ORQ_RAY_STORAGE_PATH=$PWD/0.99.0
```

In order to not clutter your local environment, it's a good idea to set these variables too:

```
export ORQ_DB_PATH=/tmp/workflows.db 
export ORQ_RAY_TEMP_PATH=/tmp/ray/temp
export ORQ_RAY_PLASMA_PATH=/tmp/ray/plasma
```

Then start the cluster:

```
orq up
```

Finally, submit the workflows:

```
orq wf submit -c local workflows multi_json_wf
orq wf submit -c local workflows multi_pickle_wf
orq wf submit -c local workflows single_json_wf
orq wf submit -c local workflows single_pickle_wf
```

After the workflows are finished, run this script to rename the workflows:
```
bash ./rename_workflows.sh ./0.99.0
```
