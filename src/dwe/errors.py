class TaskError(Exception):
    """Raise from a task handler to fail with a named error that Retry and Catch can match."""

    def __init__(self, error: str, cause: str = "") -> None:
        super().__init__(f"{error}: {cause}" if cause else error)
        self.error = error
        self.cause = cause


class StuckError(RuntimeError):
    """The execution is not finished, but nothing is runnable and nothing is scheduled."""
