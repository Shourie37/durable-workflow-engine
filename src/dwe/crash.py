class SimulatedCrash(BaseException):
    """A fake process death.

    It derives from BaseException so that handler code with `except Exception` cannot swallow it.
    """


class CrashInjector:
    """Counts named crash points. Raises SimulatedCrash at the N-th point, or at the first point named `on`."""

    def __init__(self, at: int | None = None, on: str | None = None) -> None:
        self.at = at
        self.on = on
        self.count = 0
        self.hits: list[str] = []

    def point(self, name: str) -> None:
        self.count += 1
        self.hits.append(name)
        if self.count == self.at or name == self.on:
            self.at = self.on = None
            raise SimulatedCrash(f"crash point {self.count}: {name}")
