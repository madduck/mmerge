from collections.abc import AsyncGenerator, Callable, Iterable
from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Protocol

import click
from click_async_plugins import CliContext as _CliContext
from deepmerge import always_merger
from pydantic import (
    BaseModel,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_serializer,
)


def walk_dict_recursive(d: dict[str, Any], visit_fn: Callable[[Any], Any]) -> None:
    for key, value in d.items():
        if isinstance(value, dict):
            walk_dict_recursive(value, visit_fn)

        else:
            d[key] = visit_fn(value)


class Record(BaseModel):
    srcname: str
    data: dict[str, Any] = {}
    meta: dict[str, Any] | None = None

    @field_serializer("data", mode="wrap")
    def data_serializer(
        self,
        data: dict[str, Any],
        handler: SerializerFunctionWrapHandler,
        info: SerializationInfo,
    ) -> dict[str, Any]:
        context = info.context or {}

        def visit_fn(inp: Any) -> Any:
            for filter in context.get("filters", []):
                try:
                    inp = filter(inp)

                except Exception:
                    pass

            return inp

        ret: dict[str, Any] = handler(data)
        walk_dict_recursive(ret, visit_fn)
        return ret


type Source = AsyncGenerator[Record]


class Sink(Protocol):
    async def process_record(self, record: Record) -> None: ...


@dataclass
class CliContext(_CliContext):
    srcs: list[Source] = field(default_factory=list)
    snks: list[Sink] = field(default_factory=list)


pass_clictx = click.make_pass_decorator(CliContext)


def paths_to_incdict(paths: Iterable[str]) -> dict[str, Any]:
    incdict: dict[str, Any] = {"data": True}
    for path in paths:
        newdata: dict[str, Any] = reduce(
            lambda acc, part: {part: acc or True}, path.split(".")[::-1], {}
        )
        incdict = always_merger.merge(
            incdict,
            {"data": newdata},
        )
    return incdict
