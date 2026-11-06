from pathlib import Path

DIR = Path(__file__).parent


def path(name: str) -> Path:
    return DIR / f"{name}.json"
