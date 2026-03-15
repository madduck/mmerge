import datetime
import logging
import pathlib
from contextlib import asynccontextmanager

import aiofiles
import click
from click_async_plugins import PluginLifespan, plugin
from jinja2 import Environment, FileSystemLoader

from .util import CliContext, Record, pass_clictx

logger = logging.getLogger(__name__)


class Jinja2Sink:
    def __init__(
        self, tplpath: pathlib.Path, outdir: pathlib.Path, *, fnametpl: str = ".txt"
    ) -> None:
        tplenv = Environment(loader=FileSystemLoader("."), enable_async=True)
        self._template = tplenv.get_template(str(tplpath))
        logger.info(f"Loaded template: {tplpath}")
        self._outdir = outdir
        outdir.mkdir(exist_ok=True, parents=True)
        self._fnametpl = fnametpl
        self._count = 0

    async def _instantiate(self, record: Record) -> str:
        return await self._template.render_async(
            **record.data
            | {
                "_meta": record.meta,
                "_srcname": record.srcname,
                "_now": datetime.datetime.now(),
            }
        )

    async def process_record(self, record: Record) -> None:
        self._count += 1
        async with aiofiles.open(
            (self._outdir / self._fnametpl.format(id=self._count)), "w"
        ) as outf:
            await outf.write(await self._instantiate(record))


@asynccontextmanager
async def setup_jinja2_sink(
    clictx: CliContext,
    tplpath: pathlib.Path,
    outdir: pathlib.Path,
    filename_template: str = "{id:03d}",
) -> PluginLifespan:
    clictx.snks.append(Jinja2Sink(tplpath, outdir, fnametpl=filename_template))
    yield


@plugin
@click.option(
    "--template",
    "-t",
    type=click.Path(path_type=pathlib.Path),
    metavar="JINJA2_TEMPLATE",
    help="Jinja2 template to load",
)
@click.option(
    "--outdir",
    "-o",
    type=click.Path(path_type=pathlib.Path),
    metavar="OUTPUT_DIRECTORY",
    default=pathlib.Path.cwd() / "j2out",
    help="Directory where to write files to",
)
@click.option(
    "--filename-template",
    "-f",
    default="{id:03d}.txt",
    show_default=True,
    help="File name template for generated output files",
)
@pass_clictx
async def plugincmd(
    clictx: CliContext,
    template: pathlib.Path,
    outdir: pathlib.Path,
    filename_template: str,
) -> PluginLifespan:
    """[out] Instantiate a Jinja2 template for records"""

    async with setup_jinja2_sink(clictx, template, outdir, filename_template) as task:
        yield task
