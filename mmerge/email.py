import email
import email.message
import email.policy
import logging
import pathlib
from contextlib import asynccontextmanager

import click
import markdownmail  # type: ignore[import-untyped,unused-ignore]
from click_async_plugins import PluginLifespan, plugin

from .jinja2 import Jinja2Sink
from .util import CliContext, Record, pass_clictx

logger = logging.getLogger(__name__)


class EmailSink(Jinja2Sink):
    def __init__(
        self, tplpath: pathlib.Path, outdir: pathlib.Path, *, fnametpl: str = ".txt"
    ) -> None:
        super().__init__(tplpath, outdir, fnametpl=fnametpl)

    async def _instantiate(self, record: Record) -> str:
        instance = await super()._instantiate(record)
        eml = email.message_from_string(
            instance, _class=email.message.EmailMessage, policy=email.policy.SMTPUTF8
        )
        mdn = markdownmail.MarkdownMail(content=eml.get_payload())
        eml.make_alternative()
        for part in mdn.to_mime_message().get_payload():
            eml.attach(part)
        return eml.as_string(policy=email.policy.SMTP)


@asynccontextmanager
async def setup_email_sink(
    clictx: CliContext,
    tplpath: pathlib.Path,
    outdir: pathlib.Path,
    filename_template: str = "{id:03d}",
) -> PluginLifespan:
    clictx.snks.append(EmailSink(tplpath, outdir, fnametpl=filename_template))
    yield None


@plugin
@click.option(
    "--template",
    "-t",
    type=click.Path(path_type=pathlib.Path),
    metavar="JINJA2_TEMPLATE",
    help="Jinja2 template to load for the mail body",
)
@click.option(
    "--outdir",
    "-o",
    type=click.Path(path_type=pathlib.Path),
    metavar="OUTPUT_DIRECTORY",
    default=pathlib.Path.cwd() / "emailout",
    help="Directory where to write email files to",
)
@click.option(
    "--filename-template",
    "-f",
    default="{id:03d}.eml",
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
    """[out] Create email files for records"""

    async with setup_email_sink(clictx, template, outdir, filename_template) as task:
        yield task
