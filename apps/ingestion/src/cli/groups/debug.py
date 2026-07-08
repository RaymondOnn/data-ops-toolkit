# # apps/ingestion/src/cli/commands/debug.py
# from typing import Annotated

# import typer
# from loguru import logger

# debug_app = typer.Typer(help="Debugging utilities")

# @debug_app.command("handler")
# def debug_handler_cmd(
#     path: Annotated[str, typer.Argument(help="Path to file or directory")],
#     env: str = "local",
# ):
#     """Debug format handler detection."""
#     logger.info(f"🔍 Debugging handler for: {path}")

#     from libs.file.base import FileSystemSkills, create_fs_client

#     client = create_fs_client(
#         url=path, capabilities={FileSystemSkills.FILE}, options={}
#     )

#     if hasattr(client, "get_handler"):
#         handler = client.get_handler(path)
#         logger.info(f"Handler: {type(handler).__name__}")
#         logger.info(f"FS: {type(handler.fs).__name__}")

#         files = handler.discover(path)
#         logger.info(f"Discovered {len(files)} files")
#         for f in list(files)[:10]:
#             logger.info(f"  - {f}")
#     else:
#         logger.error("Client does not support get_handler")
