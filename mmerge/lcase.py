import logging
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager

import click
from aiostream.stream import merge
from click_async_plugins import PluginLifespan, plugin
from deepmerge import always_merger

from .util import CliContext, Record, Source, pass_clictx, paths_to_incdict

logger = logging.getLogger(__name__)


async def lowercase_generator(
    srcs: Iterable[Source],
    *,
    paths: Iterable[str] | None = None,
) -> AsyncGenerator[Record]:
    incdict = paths_to_incdict(paths or [])

    async with merge(*srcs).stream() as records:
        async for record in records:
            rec = record.model_dump()
            mod = record.model_dump(include=incdict, context={"filters": [str.lower]})
            rec = always_merger.merge(rec, mod)
            yield Record.model_validate(rec)


@asynccontextmanager
async def setup_lcase_filter(
    clictx: CliContext, *, paths: set[str] | None = None
) -> PluginLifespan:
    srcs = clictx.srcs[:]
    clictx.srcs.clear()
    clictx.srcs.append(lowercase_generator(srcs, paths=paths))
    logger.info(f"Set up lowercasing for paths: {', '.join(paths) if paths else None}")
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
    """[flt] Lower-case record values for paths"""

    async with setup_lcase_filter(clictx, paths=paths) as task:
        yield task
