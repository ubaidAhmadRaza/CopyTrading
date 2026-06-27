"""
services/dashboard.py
Live terminal dashboard using Rich — reads ONLY from SQLite. It never connects
to MT5, so it can run as an independent process (or none at all) without
affecting the single-connection-per-process rule.
"""

from __future__ import annotations
import threading
import time
from datetime import datetime, timezone

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box

from database.db import Database


class Dashboard:
    def __init__(self, db: Database, refresh_s: float = 2.0):
        self.db = db
        self.refresh_s = refresh_s
        self._running = False
        self._thread: threading.Thread | None = None
        self._console = Console()

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="Dashboard")
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)

    def run_blocking(self):
        """Run the dashboard in the calling thread (for the standalone process)."""
        self._running = True
        self._run()

    def _run(self):
        with Live(console=self._console, refresh_per_second=1, screen=False) as live:
            while self._running:
                try:
                    live.update(self._build_layout())
                except Exception as exc:  # never let a render error kill the loop
                    live.update(Panel(Text(f"render error: {exc}", style="red")))
                time.sleep(self.refresh_s)

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(self._header(), size=3),
            Layout(self._accounts_panel(), size=8),
            Layout(self._stats_panel(), size=8),
            Layout(self._recent_executions(), size=20),
        )
        return layout

    def _header(self) -> Panel:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return Panel(
            Text(f"⚡ MT5 Trade Copier  |  {ts}", justify="center", style="bold green"),
            style="bold green",
        )

    def _accounts_panel(self) -> Panel:
        accounts = self.db.get_accounts()
        beats = {b["component"]: b for b in self.db.get_heartbeats()}

        t = Table(box=box.SIMPLE, expand=True)
        t.add_column("Role", style="bold cyan")
        t.add_column("Login")
        t.add_column("Server")
        t.add_column("Balance")
        t.add_column("Equity")
        t.add_column("Status")

        for a in accounts:
            comp = "master" if a["role"] == "master" else f"slave:{a['login']}"
            hb = beats.get(comp)
            status = hb["status"] if hb else "—"
            style = "green" if status == "ALIVE" else "red"
            t.add_row(
                a["role"], str(a["login"]), a["server"],
                f"{a['balance']:,.2f}", f"{a.get('equity', 0):,.2f}",
                f"[{style}]{status}[/{style}]",
            )
        return Panel(t, title="[bold]Accounts[/bold]", border_style="blue")

    def _stats_panel(self) -> Panel:
        stats = self.db.get_stats()
        queue = self.db.get_queue_depth()

        t = Table(box=box.SIMPLE, show_header=False, expand=True)
        t.add_column("Key", style="bold cyan", width=25)
        t.add_column("Value", style="white")
        t.add_row("Open Trades", str(stats["open_trades"]))
        t.add_row("Closed Trades", str(stats["closed_trades"]))
        t.add_row("Successful Executions", f"[green]{stats['successful_executions']}[/green]")
        t.add_row("Failed Executions", f"[red]{stats['failed_executions']}[/red]")
        q = " | ".join(f"{k}:{v}" for k, v in sorted(queue.items())) or "empty"
        t.add_row("Queue", q)
        return Panel(t, title="[bold]Status[/bold]", border_style="blue")

    def _recent_executions(self) -> Panel:
        rows = self.db.get_recent_executions(limit=15)
        t = Table(box=box.SIMPLE_HEAVY, expand=True, show_lines=False)
        t.add_column("Time", style="dim", width=20)
        t.add_column("Slave", width=12)
        t.add_column("Master Ticket", width=14)
        t.add_column("Symbol", width=10)
        t.add_column("Action", width=16)
        t.add_column("Status", width=10)
        t.add_column("RC", width=6)
        t.add_column("Latency", width=10)
        t.add_column("Error", style="red")

        for r in rows:
            status_style = {"SUCCESS": "green", "FAILED": "red"}.get(r["status"], "yellow")
            latency = f"{r['latency_ms']:.0f} ms" if r["latency_ms"] else "—"
            t.add_row(
                r["timestamp"][:19],
                str(r["slave_login"]),
                str(r["master_ticket"]),
                r["symbol"],
                r["action"],
                f"[{status_style}]{r['status']}[/{status_style}]",
                str(r.get("retcode") or "—"),
                latency,
                r["error"] or "",
            )
        return Panel(t, title="[bold]Recent Executions[/bold]", border_style="blue")
