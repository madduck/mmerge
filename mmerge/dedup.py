import logging
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager

import click
from aiostream.stream import merge
from click_async_plugins import PluginLifespan, plugin

from .util import CliContext, Record, Source, pass_clictx, paths_to_incdict

logger = logging.getLogger(__name__)


async def deduplicated_generator(
    srcs: Iterable[Source],
    *,
    paths: Iterable[str] | None = None,
) -> AsyncGenerator[Record]:
    seen = set[str]()

    incdict = paths_to_incdict(paths or [])

    async with merge(*srcs).stream() as records:
        async for record in records:
            mark = record.model_dump_json(include=incdict)
            if mark in seen:
                continue
            yield record
            seen.add(mark)


@asynccontextmanager
async def setup_dedup_filter(
    clictx: CliContext, *, paths: set[str] | None = None
) -> PluginLifespan:
    srcs = clictx.srcs[:]
    clictx.srcs.clear()
    clictx.srcs.append(deduplicated_generator(srcs, paths=paths))
    logger.info(
        f"Set up deduplication with paths: {', '.join(paths) if paths else None}"
    )
    yield None


@plugin
@click.option(
    "--path",
    "-p",
    "paths",
    multiple=True,
    type=str,
    help="Path(s) to add to deduplication selector",
)
@pass_clictx
async def plugincmd(clictx: CliContext, paths: set[str]) -> PluginLifespan:
    """[flt] Deduplicate records given paths into their data"""

    async with setup_dedup_filter(clictx, paths=paths) as task:
        yield task
