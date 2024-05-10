import argparse
from typing import Optional, Union

from itllib import Itl
from itllib.controllers import (
    PropagationOperator,
    PropagableConfig,
    ConfigUri,
)
from huggingface_hub import HfApi
from datasets import load_dataset
import httpx
from pydantic import BaseModel

from .query import QueryFilter


class HuggingFaceCommit(BaseModel):
    repo: str
    revision: str


class FimfarchiveSpec(BaseModel):
    upstream: Optional[Union[ConfigUri, HuggingFaceCommit]] = None  # input dataset
    filters: Optional[str] = None  # transformations
    repo: Optional[str] = None  # output dataset


class FimfarchiveStatus(BaseModel):
    lastCommit: Optional[HuggingFaceCommit] = None
    lastFilters: Optional[str] = None
    lastUpstreamCommit: Optional[HuggingFaceCommit] = None


class FimfarchiveConfig(PropagableConfig):
    spec: FimfarchiveSpec
    status: Optional[FimfarchiveStatus] = None


def get_upstream_commit(upstream) -> HuggingFaceCommit:
    if isinstance(upstream, ConfigUri):
        # Load the dataset and get the commit id
        upstream_config_dict = httpx.get(upstream.configUri).json()
        if upstream_config_dict == None:
            return None
        upstream_config = FimfarchiveConfig(**upstream_config_dict)
        if not upstream_config.status:
            return None
        if not upstream_config.status.lastCommit:
            return None

        return upstream_config.status.lastCommit

    elif isinstance(upstream, HuggingFaceCommit):
        return upstream
    else:
        raise ValueError("Invalid upstream config:", upstream)


class FimfarchiveOperator(PropagationOperator):
    CONFIG_CLS = FimfarchiveConfig
    story_filter = QueryFilter(tags_fn=lambda x: x["story_tags"].split(","))

    async def update(self, cluster: str, config: FimfarchiveConfig):
        if config.spec.repo == None:
            raise ValueError("Must specify a repo to push to")

        if config.status != None:
            if config.status.lastFilters == config.spec.filters:
                filters_changed = False
            else:
                filters_changed = True
        else:
            if config.spec.filters == None:
                filters_changed = False
            else:
                filters_changed = True

        if config.spec.upstream == None:
            if config.spec.filters != None:
                raise ValueError("Cannot apply filters without an upstream dataset")
            last_upstream_commit = None
            upstream_changed = False
        else:
            last_upstream_commit = get_upstream_commit(config.spec.upstream)
            if config.status != None:
                if last_upstream_commit == config.status.lastUpstreamCommit:
                    upstream_changed = False
                else:
                    upstream_changed = True
            else:
                if config.spec.upstream == None:
                    upstream_changed = False
                else:
                    upstream_changed = True

        if upstream_changed or filters_changed:
            dataset = load_dataset(
                last_upstream_commit.repo, revision=last_upstream_commit.revision
            )
            if config.spec.filters:
                filter_fn = self.story_filter(config.spec.filters)
                dataset = dataset["train"].filter(filter_fn)
            dataset.push_to_hub(config.spec.repo)

        api = HfApi()
        repo_info = api.repo_info(config.spec.repo, repo_type="dataset")
        last_commit = HuggingFaceCommit(repo=config.spec.repo, revision=repo_info.sha)
        return FimfarchiveStatus(
            lastCommit=last_commit,
            lastFilters=config.spec.filters,
            lastUpstreamCommit=last_upstream_commit,
        )

    async def cleanup(
        self, old_config: FimfarchiveConfig, new_config: FimfarchiveConfig | None
    ):
        if old_config.spec.repo == None:
            return

        if new_config == None or old_config.spec.repo != new_config.spec.repo:
            print("Manually delete", old_config.spec.repo)


parser = argparse.ArgumentParser()

mode_group = parser.add_argument_group("Modes")
mode_group.add_argument("--control", action="store_true", help="Start the controller")
mode_group.add_argument("--monitor", action="store_true", help="Start the monitor")
mode_group.add_argument(
    "--manual", action="store_true", help="Enable forced manual changes to configs"
)

args = parser.parse_args()

if not (args.manual) and not (args.control or args.monitor):
    parser.error("No command specified")

if (args.manual) and (args.control or args.monitor):
    parser.error("--manual cannot be combined with --control or --monitor")

itl = Itl("./loop-resources", "./loop-secrets", client="synthbot")
itl.start()
operator = FimfarchiveOperator(
    itl, "synthbot-datasets", "synthbot.fimfarchive", "v1", "Fimfarchive", "resource"
)

if args.manual:
    operator.start_manual_updates()
else:
    if args.control:
        operator.start_controller()
    if args.monitor:
        operator.start_monitor()

itl.wait()
