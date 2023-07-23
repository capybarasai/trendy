import datetime
import os

import click
from cloudpathlib import AnyPath
from loguru import logger

from sm_trendy.config import ConfigBundle
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

    cb = ConfigBundle(file_path=config_file)

    today = datetime.date.today()
    # target_folder = AnyPath("/tmp/downloaded")
    global_request_params = cb.global_config["request"]
    target_folder = cb.global_config["path"]["parent_folder"]
    trends_service = _TrendReq(
        hl=global_request_params["hl"], tz=global_request_params["tz"]
    )

    for c in cb:
        c_trend_params = c.trend_params
        c_path_params = c.path_params
        c_target_folder = c_path_params.path(parent_folder=target_folder)
        c_sdf = StoreDataFrame(target_folder=c_target_folder, snapshot_date=today)

        logger.info(
            f"keyword: {c_trend_params.keyword}\n"
            f"target_path: {c_target_folder}\n"
            "..."
        )

        st = SingleTrend(
            trends_service=trends_service,
            keyword=c_trend_params.keyword,
            geo=c_trend_params.geo,
            timeframe=c_trend_params.timeframe,
            cat=c_trend_params.cat,
        )

        c_sdf.save(st, formats=["csv", "parquet"])
