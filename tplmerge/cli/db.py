import logging
import pathlib
from contextlib import asynccontextmanager
from typing import Any, cast

import aiostream
import click
from click_async_plugins import PluginLifespan, plugin
from tptools import Court, Draw, Entry, Tournament
from tptools.util import silence_logger

from tcboard import TCBoard, TCMatch
from tcboard.ext.squore.livedata import SquoreMatchLiveData

from ..dbmanager import DBManager
from .util import CliContext, pass_clictx

DEFAULT_SQLITE_PATH = pathlib.Path("./db.sqlite")

logger = logging.getLogger(__name__)

silence_logger("aiosqlite", level=logging.WARNING)


# def _sqlite_dict_factory(cursor: Cursor, row: Row) -> dict[str, Any]:
#     fields = [column[0] for column in cursor.description]
#     return dict(zip(fields, row, strict=True))


async def receive_updates(
    clictx: CliContext,
    record_tournaments: bool = True,
    record_livedata: bool = True,
) -> None:
    sources = []
    if record_tournaments:
        sources.append(clictx.itc.updates("tournament", yield_immediately=True))
    if record_livedata:
        sources.append(clictx.itc.updates("squorelivedata", yield_immediately=True))
    if not sources:
        return

    clictx.dbmgr = cast(DBManager, clictx.dbmgr)
    async with aiostream.stream.merge(*sources).stream() as events_gen:
        async for event in events_gen:
            if event is None:
                continue
            elif isinstance(event, Tournament):
                await clictx.dbmgr.record_tournament(event)
            elif isinstance(event, SquoreMatchLiveData):
                await clictx.dbmgr.record_livedata(event)
            else:
                logger.warning(
                    "Received unexpected event "
                    f"of type {event.__class__.__qualname__}: {event}"
                )


# @asynccontextmanager
# async def _database_context(file: pathlib.Path) -> AsyncGenerator[Connection]:
#     logger.debug(f"Connecting to SQLite database {file}…")
#     # self._db = await aiosqlite.connect(self._dbname, isolation_level=None)
#     # GROSS HACK for 0.20.0 https://github.com/omnilib/aiosqlite/issues/290
#     _conn_awaitable = connect(file, isolation_level=None)
#     _conn_awaitable.daemon = True
#     async with _conn_awaitable as connection:
#         # weird typing inside aiosqlite requires cast
#         connection.row_factory = cast(type, _sqlite_dict_factory)
#         logger.info(f"Connected to SQLite database {file}")
#         yield connection
#         logger.debug(f"Disconnecting from SQLite database {file}…")
#     logger.info(f"Closed connection to SQLite database {file}")


@asynccontextmanager
async def database_backend(
    clictx: CliContext,
    *,
    file: pathlib.Path = DEFAULT_SQLITE_PATH,
    record_tournament_data: bool = True,
    record_livedata: bool = True,
) -> PluginLifespan:
    async with DBManager(file=file) as db:
        clictx.dbmgr = db
        await db.init_tables()

        if (dbboard := await db.get_last_board()) is not None:
            await clictx.board.update(dbboard)
            logger.info(
                f"Initialised board for {dbboard.tournament} "
                f"from database (rev={dbboard.rev})"
            )

        else:
            should_notify = False
            async with clictx.board.disable_updates():
                if (tjson := await db.get_latest_tournament_json()) is not None:
                    tournament = Tournament[
                        Entry, Draw, Court, TCMatch
                    ].model_validate_json(tjson)
                    await clictx.board.process_tournament(tournament)
                    should_notify = True
                    logger.info(f"Read tournament from database: {tournament!r}")

                cnt = 0
                async for ldjson in db.get_latest_livedata_json_for_each_match():
                    livedata = SquoreMatchLiveData.model_validate_json(ldjson)
                    await clictx.board.process_squore_livedata(livedata)
                    cnt += 1
                else:
                    should_notify = True
                    logger.info(f"Read {cnt} records of livedata from database")

            if should_notify:
                await clictx.board.update()

        async def record_board_state(board: TCBoard, **_: Any) -> None:
            await db.record_board_state(board)

        clictx.board.register_update_function(record_board_state)

        yield receive_updates(
            clictx,
            record_tournaments=record_tournament_data,
            record_livedata=record_livedata,
        )


@plugin
@click.option(
    "--file",
    "-f",
    metavar="SQLITE_DATABASE",
    default=DEFAULT_SQLITE_PATH,
    type=click.Path(path_type=pathlib.Path),
    show_default=True,
    help="SQLite database to persist events",
)
@click.option(
    "--record-tournament-data",
    is_flag=True,
    help="Record tournament data as it is received",
)
@click.option(
    "--record-livedata",
    is_flag=True,
    help="Record livedata data as it is received",
)
@pass_clictx
async def db(
    clictx: CliContext,
    file: pathlib.Path,
    record_tournament_data: bool,
    record_livedata: bool,
) -> PluginLifespan:
    """Listen to live Squore match data on MQTT"""

    async with database_backend(
        clictx,
        file=file,
        record_tournament_data=record_tournament_data,
        record_livedata=record_livedata,
    ) as task:
        yield task
