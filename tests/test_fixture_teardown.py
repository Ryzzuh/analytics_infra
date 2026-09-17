"""The test suite's own teardown discipline.

These assert on the fixtures rather than on the platform, because the suite was leaving
embedded Postgres servers running — 32 of them on one machine, which is every System V shared
memory id macOS allows (`kern.sysv.shmmni: 32`). Past that limit initdb fails with "No space
left on device" and every test needing a database errors out, with nothing pointing at previous
*test runs* as the cause.
"""

from __future__ import annotations

import json
import os

from conftest import server_dir

HANDLES = ".handle_pids.json"


def test_the_data_directory_is_stable_across_runs():
    """A fresh directory per run means a fresh server per run.

    Teardown runs through atexit, which SIGKILL skips — a CI timeout, a stopped run, an
    interrupted session. Each of those used to strand a server that nothing would ever reclaim,
    so the count only grew. One directory per role bounds it at one.
    """
    assert server_dir("pg") == server_dir("pg")
    assert server_dir("pg") != server_dir("app")


def test_dead_handles_are_pruned(tmp_path, monkeypatch):
    """pgserver stops a server only when the handle list holds nothing but the caller's pid.

    The list is plain JSON on disk and is never checked for liveness, so a killed run leaves its
    pid behind permanently. Every later run then concludes another process still needs the
    server and declines to stop it: one killed run disables teardown for good.
    """
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

    pgdata = tmp_path / "analytics-infra-pg"
    pgdata.mkdir()
    dead = 999_999_999  # far above any live pid on a normal system
    (pgdata / HANDLES).write_text(json.dumps([dead, os.getpid()]))

    server_dir("pg")

    remaining = json.loads((pgdata / HANDLES).read_text())
    assert dead not in remaining, "a pid that no longer exists still pins the server"
    assert os.getpid() in remaining, "a live handle must not be discarded"


def test_pruning_survives_a_corrupt_handle_file(tmp_path, monkeypatch):
    """A half-written file must not take the whole suite down.

    The handle list is written without an atomic rename, so a process killed mid-write can
    leave invalid JSON. Failing here would turn a stale file into "no tests can run at all".
    """
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

    pgdata = tmp_path / "analytics-infra-pg"
    pgdata.mkdir()
    (pgdata / HANDLES).write_text("[123, 4")  # truncated mid-write

    assert server_dir("pg") == pgdata  # no exception


def test_a_missing_handle_file_is_fine(tmp_path, monkeypatch):
    """The first run on a machine has no handle list yet."""
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

    created = server_dir("app")

    assert created.is_dir()
    assert not (created / HANDLES).exists()
