# needed < 3.14 so that annotations aren't evaluated
from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Never, cast

import click
from click_async_plugins import CliContext as _CliContext
from fastapi import Depends, FastAPI, HTTPException
from fastapi.requests import HTTPConnection
from starlette.status import HTTP_424_FAILED_DEPENDENCY
from tptools import Court

from tcboard.matchstate import TCTournament

from ..board import TCBoard
from ..dbmanager import DBManager

# TODO: for now, we only support Squore…
from ..ext.squore.livedata import SquoreMatchLiveData

if TYPE_CHECKING:
    from .ws import WSHandler

type ProcessTournamentCallable[TournamentT] = Callable[
    [TournamentT], Coroutine[None, None, None]
]
type ProcessLiveDataCallable = Callable[
    [SquoreMatchLiveData], Coroutine[None, None, None]
]


@dataclass
class CliContext(_CliContext):
    board: TCBoard = field(default_factory=TCBoard)
    api: FastAPI = field(default_factory=FastAPI)
    mqttlisteners: set[str] = field(default_factory=set)
    websockets: list[WSHandler] = field(default_factory=list)
    dbmgr: DBManager | None = None

    def __post_init__(self) -> None:
        self.api.state.clictx = self


pass_clictx = click.make_pass_decorator(CliContext)


def get_clictx(httpcon: HTTPConnection) -> CliContext:  # pragma: nocover
    return cast(CliContext, httpcon.app.state.clictx)


def get_board(clictx: Annotated[CliContext, Depends(get_clictx)]) -> TCBoard:
    return clictx.board


def get_tournament(
    board: Annotated[TCBoard, Depends(get_board)],
) -> TCTournament | Never:
    if board.tournament is None:
        raise HTTPException(
            status_code=HTTP_424_FAILED_DEPENDENCY,
            detail="Tournament not loaded",
        )
    return board.tournament


def get_courts(
    tournament: Annotated[TCTournament, Depends(get_tournament)],
) -> list[Court]:
    return tournament.get_courts()
