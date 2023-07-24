import datetime
import os

import click
from cloudpathlib import AnyPath
from loguru import logger

from sm_trendy.config import ConfigBundle
from sm_trendy.get_trends import Download, SingleTrend, StoreDataFrame, _TrendReq


@click.group(invoke_without_command=True)
@click.pass_context
def trendy(ctx):
    if ctx.invoked_subcommand is None:
        click.echo("Hello {}".format(os.environ.get("USER", "")))
    else:
        click.echo("Loading Service: %s" % ctx.invoked_subcommand)


@trendy.command()
@click.argument("config-file", type=click.Path(exists=True))
def download(config_file: AnyPath):
    """Download trends based on the config file

    :param config_file: location of a config file that contains
        the configurations and the keywords
    """
    click.echo(click.format_filename(config_file))

    cb = ConfigBundle(file_path=config_file)

    today = datetime.date.today()
    global_request_params = cb.global_config["request"]
    parent_folder = cb.global_config["path"]["parent_folder"]
    trends_service = _TrendReq(
        hl=global_request_params["hl"], tz=global_request_params["tz"]
    )

    dl = Download(
        parent_folder=parent_folder,
        snapshot_date=today,
        trends_service=trends_service,
    )

    for c in cb:
        dl(c)
