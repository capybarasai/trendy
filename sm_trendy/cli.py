import datetime
import os

import click
from cloudpathlib import AnyPath
from loguru import logger

from sm_trendy.get_trends import SingleTrend, StoreDataFrame, _TrendReq


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

    today = datetime.date.today()
    target_folder = "/tmp/downloaded"

    trends_service = _TrendReq(hl="en-US", tz=120)

    keyword = "phone case"
    st = SingleTrend(
        trends_service=trends_service,
        keyword=keyword,
        geo="DE",
        timeframe="today 5-year",
        cat=0,
    )

    sdf = StoreDataFrame(target_folder=target_folder, snapshot_date=today)
