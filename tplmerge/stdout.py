import logging
import sys
from contextlib import asynccontextmanager
from typing import TextIO

import click
from click_async_plugins import PluginLifespan, plugin
from pydantic import BaseModel

from .util import CliContext, Record, pass_clictx

logger = logging.getLogger(__name__)


class JSONDumpSink:
    def __init__(self, file: TextIO = sys.stdout, delim: str = "\n") -> None:
        self._file = file
        self._delim = delim

    def _write(self, data: BaseModel) -> None:
        self._file.write(data.model_dump_json() + self._delim)


class StdoutSink(JSONDumpSink):
    async def process_record(self, record: Record) -> None:
        self._write(record)


@asynccontextmanager
async def stdout_writer(clictx: CliContext, *, delimiter: str = "\n") -> PluginLifespan:
    clictx.snks.append(StdoutSink())
    yield


@plugin
@click.option(
    "--zero-delimited",
    "-z",
    is_flag=True,
    help="Delimit records with byte 0",
)
@pass_clictx
async def plugincmd(
    clictx: CliContext,
    zero_delimited: bool,
) -> PluginLifespan:
    """[out] Write record data to stdout as JSON"""

    async with stdout_writer(
        clictx,
        delimiter="\0" if zero_delimited else "\n",
    ) as task:
        yield task
