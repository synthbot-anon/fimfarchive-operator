# Fimfarchive Operator

This project is for deriving & maintaining datasets from the [Fimfarchive](https://www.fimfiction.net/user/116950/Fimfarchive).
It requires some initial dataset uploaded to HuggingFace. Check the notebookes in [horsewords-lib](https://github.com/synthbot-anon/horsewords-lib) for how to do that, or use an [existing repo](https://huggingface.co/datasets/synthbot/fimfarchive) with the data.


## Installation
`pip install --upgrade git+https://github.com/synthbot-anon/fimfarchive-operator`


## Overview
This project lets you derive & maintain datasets via yaml configuration files. The generated datasets are automatically synced with HuggingFace.

Here's an example configuration file that points to a manually-maintained repo:
```yaml
apiVersion: synthbot.fimfarchive/v1
kind: Fimfarchive
metadata:
  name: 'fimfarchive'
spec:
  # HuggingFace repo identifier
  repo: "synthbot/fimfarchive"
refs:
  notification:
    streamUri: "stream://dataset-streams/fimfarchive"
```

Once you have the operator running, you can apply this config file via:
```bash
# I'll make the cluster & client name more generic later. They're hardcoded in __main__.py right now.
python3 -m loopctl apply --cluster synthbot-datasets --client synthbot /path/to/config.yaml
```

Here's an example config file for deriving a dataset from an existing one:
```yaml
apiVersion: synthbot.fimfarchive/v1
kind: Fimfarchive
metadata:
  name: 'recent-twilight-stories'
spec:
  repo: "synthbot/fimfarchive-recent-twilight"
  filters: Twilight Sparkle, .created > "2023-01-01"
refs:
  upstream:
    configUri: "config://synthbot-datasets/synthbot.fimfarchive/v1/Fimfarchive/fimfarchive"
  notification:
    streamUri: "stream://dataset-streams/twilight"
```

You can update a config by `apply`ing a new one to the same cluster with the same apiVersion, kind, and name as an existing one.


## Running the operator
The operator consists of two parts: a `controller` and a `monitor`. The controller is the one that actually creates & updates the datasets. The monitor watching for upstream changes and queues update requests for the controller.

Before running either, you'll need to create the required resources.
```bash
python3 -m loopctl apply loop_config.yaml
```

This will place the corresponding config files in `loop-resources/` and `loop-secrets/`. For all subsequent commands, those two folders need to be in the current working directory.

If you want to run this in a single process, you can run both with:
```bash
python3 -m fimfarchive_operator --control --monitor
```

If you're maintaining a lot of datasets and are bottlenecked, you can run a second controller with:
```bash
python3 -m fimfarchive_operator --control
```

You probably won't get bottlenecked on monitors, but you can run multiple monitors as well for robustness.




