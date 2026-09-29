"""Emit primed process-tree resource samples from a shared PID namespace.

This module is launched in a short-lived monitoring container with
``--pid=container:<target>``.  That lets one implementation measure every
persistent service (including PostgreSQL, whose image does not contain
Python/psutil) without changing the service being measured.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProcessTreeSampler:
    """Sample PID 1 and descendants, excluding the sampler itself.

    ``psutil.cpu_percent(None)`` is deliberately called once during priming
    and newly discovered processes are likewise primed before their first CPU
    contribution.  Therefore no meaningless first CPU reading is emitted.
    """

    def __init__(self, service: str, container_id: str, interval_seconds: float, output_file: Path | None = None) -> None:
        self.service = service
        self.container_id = container_id
        self.interval_seconds = interval_seconds
        self._sampler_pid = os.getpid()
        self._primed_pids: set[int] = set()
        # cpu_percent(None) keeps its previous CPU/time counters on the
        # Process instance.  Retain instances across ticks; recreating them
        # would turn every observation into a first (meaningless) reading.
        self._processes_by_pid: dict[int, psutil.Process] = {}
        self._cgroup_memory_path = self._target_cgroup_memory_path()
        self._cgroup_cpu_path = self._cgroup_memory_path.parent / "cpu.stat"
        self._last_cpu_usage_usec: int | None = None
        self._last_cpu_monotonic: float | None = None
        self._last_diagnostic_at = 0.0
        self._last_diagnostic_rss: int | None = None
        self._stop = threading.Event()
        self._output_file = output_file
        self._output_handle = None

    def _emit(self, payload: dict[str, object], *, stdout: bool = False) -> None:
        """Persist each event without relying on the attached Docker CLI pipe.

        Docker Desktop can pause or back-pressure an attached ``docker run``
        client independently of the monitored containers.  A line-buffered
        bind-mounted file keeps the hot sampling path entirely in the Docker
        VM; the host reads it only after monitoring stops.  The ready event is
        also sent to stdout for the short startup handshake.
        """
        encoded = json.dumps(payload, sort_keys=True)
        if self._output_handle is not None:
            self._output_handle.write(encoded + "\n")
        if stdout:
            print(encoded, flush=True)

    @staticmethod
    def _target_cgroup_memory_path() -> Path:
        """Find PID 1's cgroup-v2 memory counter in the host hierarchy."""
        for line in Path("/proc/1/cgroup").read_text(encoding="utf-8").splitlines():
            if line.startswith("0::"):
                relative_path = line.split("::", 1)[1].lstrip("/")
                candidate = Path("/sys/fs/cgroup") / relative_path / "memory.current"
                if candidate.is_file():
                    return candidate
        raise RuntimeError("target cgroup-v2 memory.current is unavailable")

    def _tree(self) -> list[psutil.Process]:
        try:
            processes = [psutil.Process(1), *psutil.Process(1).children(recursive=True)]
        except psutil.Error:
            return []
        unique: dict[int, psutil.Process] = {}
        for candidate in processes:
            process = self._processes_by_pid.get(candidate.pid)
            try:
                if process is None or process.create_time() != candidate.create_time():
                    process = candidate
                    self._processes_by_pid[candidate.pid] = process
                    self._primed_pids.discard(candidate.pid)
            except psutil.Error:
                continue
            if process.pid != self._sampler_pid:
                unique[process.pid] = process
        active_pids = set(unique)
        self._processes_by_pid = {pid: process for pid, process in self._processes_by_pid.items() if pid in active_pids}
        self._primed_pids.intersection_update(active_pids)
        return list(unique.values())

    def _prime(self) -> None:
        for process in self._tree():
            try:
                process.cpu_percent(None)
                self._primed_pids.add(process.pid)
            except psutil.Error:
                continue
        self._last_cpu_usage_usec = self._cgroup_cpu_usage_usec()
        self._last_cpu_monotonic = time.monotonic()

    def _cgroup_cpu_usage_usec(self) -> int:
        for line in self._cgroup_cpu_path.read_text(encoding="utf-8").splitlines():
            key, value = line.split(maxsplit=1)
            if key == "usage_usec":
                return int(value)
        raise RuntimeError("target cgroup-v2 cpu.stat usage_usec is unavailable")

    def _cgroup_cpu_percent(self) -> float:
        now = time.monotonic(); usage = self._cgroup_cpu_usage_usec()
        if self._last_cpu_usage_usec is None or self._last_cpu_monotonic is None:
            self._last_cpu_usage_usec, self._last_cpu_monotonic = usage, now
            return 0.0
        elapsed = now - self._last_cpu_monotonic
        percent = 100.0 * (usage - self._last_cpu_usage_usec) / (elapsed * 1_000_000) if elapsed > 0 else 0.0
        self._last_cpu_usage_usec, self._last_cpu_monotonic = usage, now
        return max(0.0, percent)

    def _diagnostic_tree(self, processes: list[psutil.Process]) -> tuple[list[dict[str, object]], int | None]:
        """Keep process-tree inspection diagnostic and bounded, not on the hot path."""
        identities: list[dict[str, object]] = []
        now = time.monotonic()
        if now - self._last_diagnostic_at < 5.0:
            return identities, self._last_diagnostic_rss
        rss = 0
        for process in processes:
            try:
                rss += process.memory_info().rss
                if len(identities) < 32:
                    identities.append({"pid": process.pid, "name": process.name()})
            except psutil.Error:
                continue
        self._last_diagnostic_at, self._last_diagnostic_rss = now, rss
        return identities, rss

    def _sample(self) -> dict[str, object]:
        processes = self._tree()
        identities, process_tree_rss_bytes = self._diagnostic_tree(processes)
        # cgroup memory.current is the primary total-memory metric. Unlike a
        # process RSS sum, it charges each physical page once in this cgroup.
        cgroup_memory_bytes = int(self._cgroup_memory_path.read_text(encoding="utf-8").strip())
        return {
            "timestamp": _timestamp(),
            "service": self.service,
            "container_id": self.container_id,
            "process_identity": f"{self.service}:{self.container_id}",
            "processes": identities,
            "process_count": len(identities),
            "cpu": self._cgroup_cpu_percent(),
            "memory": cgroup_memory_bytes,
            "cgroup_memory_bytes": cgroup_memory_bytes,
            "memory_measurement": "cgroup_v2_memory_current",
            "process_tree_rss_bytes": process_tree_rss_bytes,
            "diagnostic_memory_measurement": "process_tree_rss_sum_may_double_count_shared_pages",
            "cpu_measurement": "primed_cgroup_v2_cpu_usage_usec_delta",
        }

    def run(self) -> None:
        if self._output_file is not None:
            self._output_file.parent.mkdir(parents=True, exist_ok=True)
            self._output_handle = self._output_file.open("a", encoding="utf-8", buffering=1)
        try:
            self._prime()
        except Exception as exc:
            self._emit({"event": "error", "service": self.service, "timestamp": _timestamp(), "phase": "prime", "error": f"{type(exc).__name__}: {exc}"}, stdout=True)
            return
        try:
            self._emit({"event": "ready", "service": self.service, "container_id": self.container_id, "timestamp": _timestamp()}, stdout=True)
            next_sample = time.monotonic() + self.interval_seconds
            while not self._stop.wait(max(0.0, next_sample - time.monotonic())):
                try:
                    self._emit(self._sample())
                except Exception as exc:
                    self._emit({"event": "error", "service": self.service, "timestamp": _timestamp(), "phase": "sample", "error": f"{type(exc).__name__}: {exc}"})
                next_sample += self.interval_seconds
                now = time.monotonic()
                if now > next_sample:
                    skipped = int((now - next_sample) // self.interval_seconds) + 1
                    self._emit({"event": "skipped_interval", "service": self.service, "timestamp": _timestamp(), "count": skipped, "reason": "sampling_overrun", "late_by_seconds": now - next_sample})
                    next_sample += skipped * self.interval_seconds
        finally:
            if self._output_handle is not None:
                self._output_handle.close()
                self._output_handle = None

    def stop(self, *_: object) -> None:
        self._stop.set()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--container-id", required=True)
    parser.add_argument("--interval-seconds", type=float, default=0.5)
    parser.add_argument("--output-file", type=Path)
    args = parser.parse_args(argv)
    if args.interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    sampler = ProcessTreeSampler(args.service, args.container_id, args.interval_seconds, args.output_file)
    signal.signal(signal.SIGTERM, sampler.stop)
    signal.signal(signal.SIGINT, sampler.stop)
    sampler.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
