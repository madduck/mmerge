import logging
import pathlib
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager

import click
from aiostream.stream import merge
from click_async_plugins import PluginLifespan, plugin

from .util import CliContext, Record, Source, pass_clictx, paths_to_incdict
from .workbook import setup_wb_reader

logger = logging.getLogger(__name__)


async def cull_filter(
    srcs: Iterable[Source],
    wbsrc: Source,
    *,
    paths: Iterable[str] | None = None,
) -> AsyncGenerator[Record]:
    incdict = paths_to_incdict(paths or [])

    def get_mark(record: Record) -> str:
        return record.model_dump_json(include=incdict, context={"filters": [str.lower]})

    seen = {get_mark(r) async for r in wbsrc}

    logger.debug("Ready to cull {len(seen)} records: {seen}")

    async with merge(*srcs).stream() as records:
        async for record in records:
            if get_mark(record) in seen:
                logger.debug(f"Culling record: {record}")
                continue
            yield record


@asynccontextmanager
async def setup_cull_filter(
    clictx: CliContext,
    filepath: pathlib.Path,
    *,
    sheets: set[str] | None = None,
    paths: set[str] | None = None,
) -> PluginLifespan:
    srcs = clictx.srcs[:]

    async with setup_wb_reader(clictx, filepath, limit_to_sheets=sheets):
        wbsrc = clictx.srcs.pop()

    clictx.srcs.clear()
    clictx.srcs.append(cull_filter(srcs, wbsrc, paths=paths))
    logger.info(
        f"Cull filter setup with records from {filepath} and paths: "
        f"{', '.join(paths) if paths else None}"
    )
    yield


@plugin
@click.option(
    "--filename", "-f", type=click.Path(path_type=pathlib.Path), help="Workbook to read"
)
@click.option(
    "--sheet",
    "-s",
    "sheets",
    multiple=True,
    help="Limit to given sheet(s)",
    metavar="SHEET",
)
@click.option(
    "--path",
    "-p",
    "paths",
    multiple=True,
    type=str,
    help="Path(s) to add to deduplication selector",
)
@pass_clictx
async def plugincmd(
    clictx: CliContext, filename: pathlib.Path, sheets: set[str], paths: set[str]
) -> PluginLifespan:
    """[flt] Cull records present in a given workbook"""

    async with setup_cull_filter(
        clictx, filepath=filename, sheets=sheets, paths=paths
    ) as task:
        yield task
