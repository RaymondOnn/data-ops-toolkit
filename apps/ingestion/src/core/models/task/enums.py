from enum import StrEnum


class TaskSignal(StrEnum):
    SYNC = "sync"
    DONE = "done"
    FAIL = "fail"
    RETRY = "retry"
