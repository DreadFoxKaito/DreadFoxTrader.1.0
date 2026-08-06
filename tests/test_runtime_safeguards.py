from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import app.main as main


class RuntimeSafeguardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="dft-runtime-test-")
        self.root = Path(self.tmp.name) / "app"
        self.scripts = self.root / "scripts"
        self.data = self.root / "data"
        self.runs = self.data / "runs"
        self.news = self.data / "assistant_news_runs"
        self.scripts.mkdir(parents=True)
        self.runs.mkdir(parents=True)
        self.news.mkdir(parents=True)
        self.dummy_script = self.scripts / "safe_dummy.py"
        self.dummy_script.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import argparse",
                    "import json",
                    "import pathlib",
                    "import time",
                    "ap = argparse.ArgumentParser()",
                    "ap.add_argument('--run-dir', required=True)",
                    "ap.add_argument('--params-json', required=True)",
                    "args = ap.parse_args()",
                    "pathlib.Path(args.run_dir, 'status.json').write_text(json.dumps({'phase': 'started'}))",
                    "time.sleep(30)",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        self.old_globals = {
            "APP_ROOT": main.APP_ROOT,
            "DATA_DIR": main.DATA_DIR,
            "RUNS_DIR": main.RUNS_DIR,
            "ASSISTANT_NEWS_RUNS_DIR": main.ASSISTANT_NEWS_RUNS_DIR,
            "DB_PATH": main.DB_PATH,
            "SCRIPTS_DIR": main.SCRIPTS_DIR,
            "RUN_MAX_AUTO_RESTARTS": main.RUN_MAX_AUTO_RESTARTS,
            "STOP_RUNS_ON_SHUTDOWN": main.STOP_RUNS_ON_SHUTDOWN,
        }
        main.APP_ROOT = self.root
        main.DATA_DIR = self.data
        main.RUNS_DIR = self.runs
        main.ASSISTANT_NEWS_RUNS_DIR = self.news
        main.DB_PATH = self.data / "cryptid_exchange.sqlite3"
        main.SCRIPTS_DIR = self.scripts
        main.RUN_MAX_AUTO_RESTARTS = 2
        main.STOP_RUNS_ON_SHUTDOWN = True

        main.init_db()
        self.conn = main.db()
        self.algo_id = self._create_algorithm(restart_on_crash=1)
        self.spawned_pids: list[int] = []

    def tearDown(self) -> None:
        for pid in self.spawned_pids:
            try:
                main._terminate_pid(pid, grace_sec=0.2)
            except Exception:
                pass
        try:
            self.conn.close()
        except Exception:
            pass
        for key, value in self.old_globals.items():
            setattr(main, key, value)
        self.tmp.cleanup()

    def _create_algorithm(self, *, restart_on_crash: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO base_scripts (name, path, description, params_schema_json, created_ts) VALUES (?,?,?,?,?)",
            ("safe dummy", "scripts/safe_dummy.py", "test", "{}", int(time.time())),
        )
        base_id = int(cur.lastrowid)
        cur.execute(
            """
            INSERT INTO algorithms
            (name, base_script_id, rulesets_json, params_json, max_runtime_min, restart_on_crash, log_level, created_ts)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            ("Recoverable Dummy", base_id, "[]", "{}", 0, int(restart_on_crash), "INFO", int(time.time())),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def _insert_run(
        self,
        *,
        pid: int | None = 999999,
        status: str = "running",
        supervisor_pid: int | None = None,
        supervisor_started_ts: int | None = None,
        restart_count: int = 0,
    ) -> int:
        run_dir = self.runs / f"run_test_{time.time_ns()}"
        run_dir.mkdir(parents=True)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO runs
            (algorithm_id, algorithm_name, params_json, run_dir, pid, status, start_ts,
             supervisor_pid, supervisor_started_ts, restart_count, last_restart_ts, restart_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self.algo_id,
                "Recoverable Dummy",
                "{}",
                str(run_dir),
                pid,
                status,
                int(time.time()) - 120,
                supervisor_pid,
                supervisor_started_ts,
                restart_count,
                None,
                None,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def _run_row(self, run_id: int) -> sqlite3.Row:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM runs WHERE id=?", (int(run_id),))
        row = cur.fetchone()
        assert row is not None
        return row

    def test_startup_reconciliation_does_not_spawn_stale_runs(self) -> None:
        for _ in range(6):
            self._insert_run()

        with mock.patch.object(main, "_spawn_run_process") as spawn:
            main._refresh_run_processes(self.conn, allow_auto_restart=False)

        spawn.assert_not_called()
        cur = self.conn.cursor()
        cur.execute("SELECT status, pid, restart_reason FROM runs ORDER BY id")
        rows = [dict(row) for row in cur.fetchall()]
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(row["status"] == "crashed" for row in rows))
        self.assertTrue(all(row["pid"] is None for row in rows))
        self.assertTrue(all(row["restart_reason"] == "passive_recovery_disabled" for row in rows))

    def test_passive_refresh_does_not_spawn_runs_from_old_server_instance(self) -> None:
        run_id = self._insert_run(supervisor_pid=12345, supervisor_started_ts=1)

        with mock.patch.object(main, "_spawn_run_process") as spawn:
            main._refresh_run_processes(self.conn)

        spawn.assert_not_called()
        row = self._run_row(run_id)
        self.assertEqual(row["status"], "crashed")
        self.assertIsNone(row["pid"])
        self.assertEqual(row["restart_reason"], "not_owned_by_current_server")

    def test_current_server_owned_dead_run_restarts_once(self) -> None:
        run_id = self._insert_run(supervisor_pid=main.os.getpid(), supervisor_started_ts=main.SERVER_STARTED_TS)

        main._refresh_run_processes(self.conn)
        row = self._run_row(run_id)
        self.spawned_pids.append(int(row["pid"]))

        self.assertEqual(row["status"], "running")
        self.assertNotEqual(int(row["pid"]), 999999)
        self.assertTrue(main._pid_is_alive(int(row["pid"])))
        self.assertEqual(int(row["restart_count"]), 1)
        self.assertEqual(row["restart_reason"], "restart_allowed")

    def test_restart_limit_marks_run_crashed_without_spinning(self) -> None:
        run_id = self._insert_run(
            supervisor_pid=main.os.getpid(),
            supervisor_started_ts=main.SERVER_STARTED_TS,
            restart_count=main.RUN_MAX_AUTO_RESTARTS,
        )

        with mock.patch.object(main, "_spawn_run_process") as spawn:
            main._refresh_run_processes(self.conn)

        spawn.assert_not_called()
        row = self._run_row(run_id)
        self.assertEqual(row["status"], "crashed")
        self.assertIsNone(row["pid"])
        self.assertEqual(row["restart_reason"], "restart_limit_reached")

    def test_shutdown_terminates_owned_run_processes(self) -> None:
        run_dir = self.runs / "run_shutdown_owned"
        run_dir.mkdir(parents=True)
        pid = main._spawn_run_process(entrypoint=str(self.dummy_script), run_dir=run_dir, params_json="{}")
        self.spawned_pids.append(pid)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO runs
            (algorithm_id, algorithm_name, params_json, run_dir, pid, status, start_ts,
             supervisor_pid, supervisor_started_ts, restart_count, last_restart_ts, restart_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self.algo_id,
                "Recoverable Dummy",
                "{}",
                str(run_dir),
                pid,
                "running",
                int(time.time()),
                main.os.getpid(),
                main.SERVER_STARTED_TS,
                0,
                None,
                "manual_start",
            ),
        )
        run_id = int(cur.lastrowid)
        self.conn.commit()

        result = main._shutdown_owned_run_processes(timeout_sec=0.2)
        row = self._run_row(run_id)

        self.assertEqual(result["runs_signaled"], 1)
        self.assertEqual(result["alive_after_shutdown"], 0)
        self.assertFalse(main._pid_is_alive(pid))
        self.assertEqual(row["status"], "stopped")
        self.assertIsNone(row["pid"])
        self.assertEqual(row["restart_reason"], "server_shutdown")


if __name__ == "__main__":
    unittest.main()
