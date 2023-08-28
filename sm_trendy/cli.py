import datetime
import json
import os
import random
import time

import click
from cloudpathlib import AnyPath, S3Path
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.prompt import Prompt

import sm_trendy.use_pytrends.config as ptc
import sm_trendy.use_pytrends.get_trends as ptg
from sm_trendy.aggregate.agg import AggAPIJSON, AggSerpAPIBundle, DownloadedLoader
from sm_trendy.manual.config import SerpAPI2Manual
from sm_trendy.manual.get_trends import ManualDownload
from sm_trendy.use_serpapi.config import SerpAPIConfigBundle
from sm_trendy.use_serpapi.get_trends import SerpAPIDownload
from sm_trendy.utilities.config import ConfigTable
from sm_trendy.utilities.request import get_random_user_agent
from sm_trendy.utilities.storage import StoreJSON

load_dotenv()


@click.group(invoke_without_command=True)
@click.pass_context
def trendy(ctx):
    if ctx.invoked_subcommand is None:
        click.echo("Hello {}".format(os.environ.get("USER", "")))
    else:
        click.echo("Loading Service: %s" % ctx.invoked_subcommand)


@trendy.command()
@click.argument("config-file", type=click.Path(exists=True))
def download_pytrends(config_file: AnyPath):
    """Download trends based on the config file

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(click.format_filename(config_file))

    cb = ptc.ConfigBundle(file_path=config_file)

    today = datetime.date.today()
    global_request_params = cb.global_config["request"]
    parent_folder = cb.global_config["path"]["parent_folder"]
    trends_service = ptg._TrendReq(
        hl=global_request_params["hl"],
        tz=global_request_params["tz"],
        timeout=(10, 14),
        requests_args={"headers": get_random_user_agent()},
        proxies=["https://157.245.27.9:3128"],
    )

    dl = ptg.Download(
        parent_folder=parent_folder,
        snapshot_date=today,
        trends_service=trends_service,
    )

    wait_seconds_min_max = (30, 120)

    for c in cb:
        dl(c)
        wait_seconds = random.randint(*wait_seconds_min_max)
        logger.info(f"Waiting for {wait_seconds} seconds ...")
        time.sleep(wait_seconds)


@trendy.command()
@click.argument("config-file", type=AnyPath)
@click.argument("wait-seconds-min", type=int, default=5)
@click.argument("wait-seconds-max", type=int, default=10)
def download_serpapi(
    config_file: AnyPath, wait_seconds_min: int, wait_seconds_max: int
):
    """Download trends based on the config file

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(click.format_filename(config_file))

    today = datetime.date.today()

    api_key = os.environ.get("SERPAPI_KEY")
    if api_key is None:
        logger.error("api_key is empty, please set the env var: " "SERPAPI_KEY")

    scb = SerpAPIConfigBundle(file_path=config_file, serpapi_key=api_key)

    parent_folder = scb.global_config["path"]["parent_folder"]
    sdl = SerpAPIDownload(parent_folder=parent_folder, snapshot_date=today)

    wait_seconds_min_max = (wait_seconds_min, wait_seconds_max)

    for c in scb:
        try:
            sdl(c)
            wait_seconds = random.randint(*wait_seconds_min_max)
            logger.info(f"Waiting for {wait_seconds} seconds ...")
            time.sleep(wait_seconds)
        except Exception as e:
            logger.error("Can not download: \n" f"config: {c}\n" f" error: {e}")


@trendy.command()
@click.argument("config-file", type=AnyPath)
@click.argument("manual-folder", type=AnyPath)
def create_manual_folders(config_file: AnyPath, manual_folder: AnyPath):
    """Create folders based on the serpapi config

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(click.format_filename(config_file))

    scb = SerpAPIConfigBundle(file_path=config_file, serpapi_key="")
    if not isinstance(manual_folder, AnyPath):
        manual_folder = AnyPath(manual_folder)
    s2m = SerpAPI2Manual(manual_folder=manual_folder)
    logger.info(f"Create intermediate folders in {manual_folder} ...")
    s2m(config_bundle=scb)


@trendy.command()
@click.argument("config-file", type=AnyPath)
@click.argument("manual-folder", type=AnyPath)
def upload_manual(config_file: AnyPath, manual_folder: AnyPath):
    """Download trends based on the config file

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(click.format_filename(config_file))

    today = datetime.date.today()

    scb = SerpAPIConfigBundle(file_path=config_file, serpapi_key="")

    parent_folder = scb.global_config["path"]["parent_folder"]
    mdl = ManualDownload(
        parent_folder=parent_folder, snapshot_date=today, manual_folder=manual_folder
    )

    for c in scb:
        try:
            mdl(c)
        except Exception as e:
            logger.error("Can not download: \n" f"config: {c}\n" f" error: {e}")


@trendy.command()
@click.argument("config-file", type=AnyPath)
@click.argument("top-n", type=int, default=10)
def validate_config(config_file: AnyPath, top_n: int):
    """Validate config file

    :param config_file: location of a config file that contains
        the configurations and the keywords
    :param top_n: top n to be displayed
    """
    click.echo(f"Validating {click.format_filename(str(config_file))} ...")

    scb = SerpAPIConfigBundle(file_path=config_file, serpapi_key="")

    ct = ConfigTable(scb)
    console = Console()
    console.print(ct.table(top_n=top_n))

    api_key = os.environ.get("SERPAPI_KEY")
    console.print(f"SERPAPI_KEY Exists: {api_key is not None}")

    for c in scb:
        try:
            c._validate()
        except Exception as e:
            logger.error(f"Keyword: {c.serpapi_params.q} validation failed: {e}")


@trendy.command()
@click.argument("config-file", type=AnyPath)
def agg(config_file: AnyPath):
    """Aggregate the downloaded results into single files

    For example, `s3://sm-google-trend/configs/aggregate_config.json`

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(f"Aggregation config: {click.format_filename(str(config_file))}")
    if not isinstance(config_file, AnyPath):
        config_file = AnyPath(config_file)

    with open(config_file, "r") as fp:
        config = json.load(fp)

    parent_folder = AnyPath(config["global"]["path"]["parent_folder"])
    agg_bundle = AggSerpAPIBundle(parent_path=parent_folder)

    for k in config["keywords"]:
        keyword_configs = AnyPath(k["config"])
        logger.info(f"Aggregating {keyword_configs}")
        agg_bundle(serpapi_config_path=keyword_configs)


@trendy.command()
@click.argument("config-file", type=AnyPath)
def agg_metadata(config_file: AnyPath):
    """Convert configs to a json file that our
    website can get a list of keywords and
    their corresponding path, for visualizations.

    `config_file` should have the same format as aggregation config, e.g., `s3://sm-google-trend/configs/aggregate_config.json`

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(f"Using aggregation config: {click.format_filename(str(config_file))}")

    if not isinstance(config_file, AnyPath):
        config_file = AnyPath(config_file)

    with open(config_file, "r") as fp:
        config = json.load(fp)

    parent_folder = AnyPath(config["global"]["path"]["parent_folder"])

    if not isinstance(parent_folder, S3Path):
        raise Exception(f"parent folder is not S3Path: {parent_folder}")

    s3_public_region = "eu-central-1"
    s3_public_folder = (
        parent_folder.key[:-1] if parent_folder.key.endswith("/") else parent_folder.key
    )
    s3_public_base_url = f"https://{parent_folder.bucket}.s3.{s3_public_region}.amazonaws.com/{s3_public_folder}"
    all_config = []
    for k in config["keywords"]:
        keyword_configs = AnyPath(k["config"])
        logger.info(f"Parsing {keyword_configs}")

        scb = SerpAPIConfigBundle(file_path=keyword_configs, serpapi_key="")

        for c in scb:
            logger.debug(f"  Converting {c.path_params}")
            # URL of the data file
            c_url = c.path_params.s3_access_point(
                base_url=s3_public_base_url,
                snapshot_date="latest",
                filename="data.json",
            )
            c_s3_path = c.path_params.s3_path(
                parent_folder=parent_folder,
                snapshot_date="latest",
                filename="data.json",
            )

            # Build Path Config
            all_config.append(
                {
                    "keyword": c.extra_metadata.get("topic") or c.path_params.keyword,
                    "cat": c.path_params.cat,
                    "geo": c.path_params.geo,
                    "timeframe": c.path_params.timeframe,
                    "path": c_url,
                    "q": c.path_params.keyword,
                    "s3": str(c_s3_path),
                }
            )

    # save all to a metadata.json file
    target_path = parent_folder / "metadata.json"
    logger.info(f"Saving metadata to {target_path} ...")
    with target_path.open("w+") as fp:
        json.dump(all_config, fp)
