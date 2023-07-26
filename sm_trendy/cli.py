import datetime
import os
import random
import time

import click
from cloudpathlib import AnyPath
from dotenv import load_dotenv
from loguru import logger

import sm_trendy.use_pytrends.config as ptc
import sm_trendy.use_pytrends.get_trends as ptg
from sm_trendy.manual.config import SerpAPI2Manual
from sm_trendy.manual.get_trends import ManualDownload
from sm_trendy.use_serpapi.config import SerpAPIConfigBundle
from sm_trendy.use_serpapi.get_trends import SerpAPIDownload
from sm_trendy.utilities.request import get_random_user_agent

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
@click.argument("config-file", type=click.Path(exists=True))
def download_serpapi(config_file: AnyPath):
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

    wait_seconds_min_max = (0, 1)

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
