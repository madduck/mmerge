import logging
import pathlib
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import click
from click_async_plugins import PluginLifespan, plugin
from pyexcel import iget_book
from pyexcel.internal import SheetStream

from .util import CliContext, Record, pass_clictx

logger = logging.getLogger(__name__)


type Row = dict[str, Any]


async def read_sheet(
    sheet: SheetStream,
    *,
    column_names_in_row: int = 0,
) -> AsyncGenerator[tuple[Row, int]]:

    colnames = None
    for n, row in enumerate(sheet.to_array()):
        if not row:
            continue

        if n < column_names_in_row:
            continue
        elif n == column_names_in_row:
            colnames = row
        else:
            if colnames is None:
                colnames = [f"col{i}" for i in range(len(row))]

            yield dict(zip(colnames, row, strict=False)), n


async def read_sheets(
    filepath: pathlib.Path, sheets: dict[str, SheetStream], metadata: dict[str, Any]
) -> AsyncGenerator[Record]:
    for name, sheet in sheets.items():
        srcname = f"{filepath}[{name}]"
        async for record, rownr in read_sheet(sheet):
            yield Record(
                srcname=srcname,
                data=record,
                meta=metadata | {"sheet": name, "row": rownr},
            )


@asynccontextmanager
async def setup_wb_reader(
    clictx: CliContext,
    filepath: pathlib.Path,
    *,
    limit_to_sheets: set[str] | None = None,
    excluded_sheets: set[str] | None = None,
) -> PluginLifespan:
    book = iget_book(file_name=filepath, renderer=lambda x: x)
    sheetsavail = book.sheet_names()

    if limit_to_sheets is None:
        sheets = {
            k: v for k, v in book.sheets.items() if k not in (excluded_sheets or {})
        }
    else:
        sheets = {}
        for sheet in limit_to_sheets:
            if sheet not in sheetsavail:
                raise click.ClickException(f"No such sheet in {filepath}: {sheet}")

            elif sheet not in (excluded_sheets or {}):
                sheets[sheet] = getattr(book, sheet)

    metadata = {"workbook": str(filepath.absolute())}

    clictx.srcs.append(read_sheets(filepath, sheets, metadata))

    logger.info(
        f"Set up to read records from {len(sheets)} sheet(s) of workbook {filepath}"
    )
    yield None


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
    "--exclude",
    "-x",
    "excludes",
    multiple=True,
    help="Exclude the given sheet(s)",
    metavar="SHEET",
)
@pass_clictx
async def plugincmd(
    clictx: CliContext, filename: pathlib.Path, sheets: set[str], excludes: set[str]
) -> PluginLifespan:
    """[src] Read records from an OpenOffice/Excel workbook"""

    async with setup_wb_reader(
        clictx, filename, limit_to_sheets=sheets or None, excluded_sheets=excludes
    ) as task:
        yield task
