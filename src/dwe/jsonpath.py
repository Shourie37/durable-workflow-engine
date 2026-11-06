"""A small subset of JSONPath, enough for state machine input and output processing.

Supported: `$`, `$$` (context object), `.field`, `['field']`, and `[index]`.
"""

import copy
import re
from typing import Any

_STEP = re.compile(r"\.([A-Za-z_][\w-]*)|\['([^']*)'\]|\[(\d+)\]")


class PathError(ValueError):
    pass


def parse(path: str) -> tuple[str, list[str | int]]:
    if path.startswith("$$"):
        root, rest = "$$", path[2:]
    elif path.startswith("$"):
        root, rest = "$", path[1:]
    else:
        raise PathError(f"path must start with $: {path!r}")
    steps: list[str | int] = []
    pos = 0
    while pos < len(rest):
        m = _STEP.match(rest, pos)
        if not m:
            raise PathError(f"unsupported path syntax: {path!r}")
        name, quoted, index = m.groups()
        steps.append(int(index) if index is not None else (name if name is not None else quoted))
        pos = m.end()
    return root, steps


def get(data: Any, path: str, context: Any = None) -> Any:
    root, steps = parse(path)
    node = context if root == "$$" else data
    for step in steps:
        try:
            node = node[step]
        except (KeyError, IndexError, TypeError):
            raise PathError(f"path {path!r} not found in input") from None
    return node


def exists(data: Any, path: str) -> bool:
    try:
        get(data, path)
        return True
    except PathError:
        return False


def set_path(data: Any, path: str | None, value: Any) -> Any:
    """Return a copy of `data` with `value` placed at `path` (ResultPath semantics)."""
    if path is None:
        return data
    root, steps = parse(path)
    if root != "$":
        raise PathError("ResultPath cannot use the context object")
    if not steps:
        return value
    out = copy.deepcopy(data) if isinstance(data, (dict, list)) else {}
    node = out
    for step in steps[:-1]:
        if isinstance(step, int):
            node = node[step]
            continue
        if not isinstance(node.get(step), (dict, list)):
            node[step] = {}
        node = node[step]
    node[steps[-1]] = value
    return out


def render(template: Any, data: Any, context: Any = None) -> Any:
    """Build a payload from a Parameters-style template. Keys ending in `.$` are paths."""
    if isinstance(template, dict):
        out = {}
        for key, value in template.items():
            if key.endswith(".$"):
                out[key[:-2]] = get(data, value, context)
            else:
                out[key] = render(value, data, context)
        return out
    if isinstance(template, list):
        return [render(v, data, context) for v in template]
    return template
