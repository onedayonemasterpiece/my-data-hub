from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from typing import Any

from .manager import ShowcaseManager


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="my-data-hub-showcase")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list")
    get_link = subparsers.add_parser("get-link")
    get_link.add_argument("view_id")
    rebuild = subparsers.add_parser("rebuild")
    rebuild.add_argument("view_id")
    create = subparsers.add_parser("create-view")
    create.add_argument("view_id")
    create.add_argument("--no-publish", action="store_true")
    rotate = subparsers.add_parser("rotate-link")
    rotate.add_argument("view_id")
    revoke = subparsers.add_parser("revoke-link")
    revoke.add_argument("view_id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    manager = ShowcaseManager.from_env()
    actions: dict[str, Callable[[], dict[str, Any]]] = {
        "list": manager.list_surfaces,
        "get-link": lambda: manager.get_link(args.view_id),
        "rebuild": lambda: manager.rebuild(args.view_id),
        "create-view": lambda: manager.create_view(args.view_id, publish=not args.no_publish),
        "rotate-link": lambda: manager.rotate_link(args.view_id),
        "revoke-link": lambda: manager.revoke_link(args.view_id),
    }
    print(json.dumps(actions[args.command](), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
