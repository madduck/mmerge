import asyncio
import importlib
import logging
import pathlib
from contextlib import AsyncExitStack
from typing import Never

import click
import click_extra as clickx
from click_async_plugins import (
    ITC,
    PluginFactory,
    create_plugin_task,
    plugin_group,
    setup_plugins,
)

from .util import CliContext, Source, pass_clictx

PLUGINS = [
    "counter",
    "workbook",
    "lcase",
    "cull",
    "dedup",
    "stdout",
    "jinja2",
    "email",
]

logger = clickx.new_extra_logger(
    format="{asctime} {name} {levelname} {message} ({filename}:{lineno})",
    datefmt="%F %T",
    level=logging.WARNING,
)


@plugin_group
@clickx.config_option(  # type: ignore[untyped-decorator]
    strict=True,
    show_default=True,
    file_format_patterns=clickx.ConfigFormat.TOML,
    default=pathlib.Path(pathlib.Path.cwd() / "tplmerge.toml"),
)
@clickx.verbose_option(default_logger=logger)  # type: ignore[untyped-decorator]
@click.pass_context
def tplmerge(
    ctx: click.Context,
) -> None:
    """Tool to merge data sources and instantiate templates"""

    itc = ITC()
    ctx.obj = CliContext(itc=itc)


for plugin in PLUGINS:
    try:
        tplmerge.add_command(plugin)  # type: ignore[arg-type]

    except AttributeError:
        try:
            mod = importlib.import_module(f".{plugin}", __package__)

        except (ImportError, NotImplementedError) as exc:
            logger.warning(f"Plugin '{plugin}' cannot be loaded: {exc}")

        else:
            # subcmd = getattr(mod, "plugincmd")
            tplmerge.add_command(mod.plugincmd, name=plugin)

    logger.debug(f"Added plugin to tplmerge: {plugin}")


async def process_records(clictx: CliContext, src: Source) -> int:
    count = 0
    async for record in src:
        logger.debug(f"Distributing record to {len(clictx.snks)} sinks: {record}")
        for snk in clictx.snks:
            await snk.process_record(record)
        count += 1
    return count


async def mainloop(clictx: CliContext, plugin_factories: list[PluginFactory]) -> None:
    async with AsyncExitStack() as stack:
        tasks = await setup_plugins(plugin_factories, stack=stack)

        try:
            async with asyncio.TaskGroup() as tg:
                for task in tasks:
                    create_plugin_task(task, create_task_fn=tg.create_task)

                try:
                    logger.info(f"Processing records from {len(clictx.srcs)} sources")
                    tasks = set[asyncio.Task]()
                    async with asyncio.TaskGroup() as srctasks:
                        for src in clictx.srcs:
                            tasks.add(
                                srctasks.create_task(
                                    process_records(clictx, src), name=f"{src}"
                                )
                            )
                    total = sum(t.result() for t in tasks)
                    logger.info(f"Processed {total} records.")

                except KeyboardInterrupt:
                    pass

                raise asyncio.CancelledError

        except asyncio.CancelledError:
            logger.info("Exiting…")


@tplmerge.result_callback()
@pass_clictx
def runit(
    clictx: CliContext,
    plugin_factories: list[PluginFactory],
) -> Never:
    try:
        asyncio.run(mainloop(clictx, plugin_factories))

    except* click.ClickException as exc:
        for e in exc.exceptions:
            raise e from exc

    except* Exception:
        logger.exception("Something went really wrong")

        try:
            import ipdb

        except ImportError:
            pass

        else:
            ipdb.set_trace()  # noqa: E402 E702 I001 # fmt: skip

        click.get_current_context().exit(1)

    click.get_current_context().exit(0)
