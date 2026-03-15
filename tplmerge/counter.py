import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import click
from click_async_plugins import PluginLifespan, plugin

from .util import CliContext, Record, pass_clictx

logger = logging.getLogger(__name__)


async def intgen(*, max: int | None = None, sleep: float = 0) -> AsyncGenerator[Record]:
    i = 0
    while max is None or i < max:
        logger.info(f"Making record for i={i}")
        yield Record(srcname="Counter", data={"value": i})
        await asyncio.sleep(sleep)
        i += 1


@asynccontextmanager
async def setup_counter(
    clictx: CliContext, *, max: int | None = None, sleep: float = 0
) -> PluginLifespan:
    clictx.srcs.append(intgen(max=max, sleep=sleep))
    logger.info(f"Counter source setup with {max=} {sleep=}")
    yield


@plugin
@click.option(
    "--max", "-m", type=click.IntRange(min=1), help="Maximum integer to count up to"
)
@click.option(
    "--sleep",
    "-s",
    type=float,
    default=click.FloatRange(min=0.01),
    help="Sleep between integers",
)
@pass_clictx
async def plugincmd(
    clictx: CliContext,
    max: int | None,
    sleep: float,
) -> PluginLifespan:
    """[src] Generate an increasing sequence of integers"""

    async with setup_counter(clictx, max=max, sleep=sleep) as task:
        yield task
