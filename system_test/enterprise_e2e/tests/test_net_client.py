"""
Deterministic failover tests for the .NET QWiP client against
QuestDB Enterprise.

Each test mirrors the pattern in questdb-ent/e2e/tests/test_failover.py:
start a primary, send rows via the .NET sidecar, kill -9 the primary,
start a successor, and verify no rows were lost.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import pytest

from lib import lifecycle as lc
from lib.obj_store import ObjStore
from lib.pg_query import count_rows, wait_for_dense_sequence
from lib.server import wait_port_free

from .fake_upgrade_server import FakeUpgradeServer

LOG = logging.getLogger(__name__)


def _connect_string(http_port: int, sf_dir: Path, *, request_durable_ack: bool = True,
                    reconnect_max_ms: int = 60_000,
                    close_flush_timeout_ms: int = 5_000) -> str:
    parts = [
        f"ws::addr=127.0.0.1:{http_port}",
        "user=admin",
        "password=quest",
        f"sf_dir={sf_dir}",
        f"reconnect_max_duration_millis={reconnect_max_ms}",
        f"close_flush_timeout_millis={close_flush_timeout_ms}",
    ]
    if request_durable_ack:
        parts.append("request_durable_ack=on")
    return ";".join(parts) + ";"


@pytest.mark.net_client
def test_kill9_primary_failover_no_data_loss(server_factory, net_sidecar,
                                              obj_store: ObjStore, scenario_dir: Path) -> None:
    """Kill -9 P1 mid-flight, verify P2 has every row."""
    table = "net_trades_failover"
    row_count = 50
    sf_dir = scenario_dir / "sf"

    p1 = server_factory("p1")
    p1_ports = p1.start()

    net_sidecar.connect(_connect_string(p1_ports.http, sf_dir))
    net_sidecar.send(table, count=row_count, start_index=0)
    net_sidecar.flush()

    time.sleep(0.5)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=row_count, timeout_s=60.0)


@pytest.mark.net_client
def test_failover_during_active_send(server_factory, net_sidecar,
                                     obj_store: ObjStore, scenario_dir: Path) -> None:
    """Kill P1 while the sender is still pushing batches."""
    table = "net_trades_inflight"
    sf_dir = scenario_dir / "sf"
    batches = 5
    rows_per_batch = 20
    expected = batches * rows_per_batch

    p1 = server_factory("p1")
    p1_ports = p1.start()
    net_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    net_sidecar.send(table, count=rows_per_batch, start_index=0)
    net_sidecar.flush()
    for i in range(1, batches):
        net_sidecar.send(table, count=rows_per_batch, start_index=i * rows_per_batch)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    net_sidecar.flush()

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=expected, timeout_s=60.0)


@pytest.mark.net_client
def test_two_failovers_in_one_scenario(server_factory, net_sidecar,
                                       obj_store: ObjStore, scenario_dir: Path) -> None:
    """Multiple failovers in a row — no row should be lost."""
    table = "net_trades_two_fail"
    sf_dir = scenario_dir / "sf"
    rows_per_phase = 25
    expected = rows_per_phase * 3

    # Phase 1.
    p1 = server_factory("p1")
    p1_ports = p1.start()
    net_sidecar.connect(_connect_string(p1_ports.http, sf_dir))
    net_sidecar.send(table, count=rows_per_phase, start_index=0)
    net_sidecar.flush()
    time.sleep(0.5)
    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)
    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    # Phase 2.
    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)
    net_sidecar.send(table, count=rows_per_phase, start_index=rows_per_phase)
    net_sidecar.flush()
    time.sleep(0.5)
    p2.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)
    if p2.db_root.exists():
        shutil.rmtree(p2.db_root)
    obj_store.wipe()

    # Phase 3.
    p3 = server_factory("p3", db_root_name="p3-fresh")
    p3.start(http_port=p1_ports.http, pg_port=p1_ports.pg)
    net_sidecar.send(table, count=rows_per_phase, start_index=rows_per_phase * 2)
    net_sidecar.flush()

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=expected, timeout_s=90.0)


@pytest.mark.net_client
def test_no_request_durable_ack_loses_rows(server_factory, net_sidecar,
                                            obj_store: ObjStore, scenario_dir: Path) -> None:
    """Without durable-ack opt-in, SF trims on OK — killing P1 loses rows.
    This is the negative case that proves the harness is wired correctly."""
    table = "net_trades_no_durable"
    sf_dir = scenario_dir / "sf"
    row_count = 50

    p1 = server_factory("p1")
    p1_ports = p1.start()

    net_sidecar.connect(_connect_string(p1_ports.http, sf_dir, request_durable_ack=False))
    net_sidecar.send(table, count=row_count, start_index=0)
    fsn = net_sidecar.flush()
    net_sidecar.await_acked(fsn, timeout_ms=30_000)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2_ports = p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    time.sleep(5)

    from lib.pg_query import execute_ddl
    import psycopg
    try:
        conn = psycopg.connect(
            f"host=127.0.0.1 port={p2_ports.pg} user=admin password=quest dbname=qdb",
            autocommit=True,
        )
        cur = conn.execute(f"SELECT count() FROM '{table}'")
        actual = cur.fetchone()[0]
        conn.close()
    except Exception:
        actual = 0

    assert actual < row_count, (
        f"Expected data loss without durable-ack but got {actual}/{row_count} rows"
    )


# ---------------------------------------------------------------------------
# Write-side failover / role-negotiation scenarios, ported from the Java client
# (java-questdb-client core/.../cutlass/qwp/client/WriteFailoverTest.java). The
# reject cases drive a self-contained FakeUpgradeServer instead of a real node,
# because the behaviour under test is entirely client-side connect-walk
# classification of the /write/v4 upgrade response — no cluster required. The
# positive walk-past case pairs a fake rejecting replica with a real primary.
# ---------------------------------------------------------------------------

def _reject_connect_string(*ports: int, blocking: bool = True, budget_ms: int = 1500) -> str:
    """A ws:: connect string over the given loopback ports.

    ``blocking`` selects ``initial_connect_retry=on`` so the SYNC initial connect
    is budget-bounded and a role-reject sweep surfaces terminally (Invariant B
    otherwise retries an all-replica window forever)."""
    addr = ",".join(f"127.0.0.1:{p}" for p in ports)
    parts = [f"ws::addr={addr}"]
    if blocking:
        parts.append("initial_connect_retry=on")
        parts.append(f"reconnect_max_duration_millis={budget_ms}")
    return ";".join(parts) + ";"


@pytest.mark.net_client
def test_single_replica_upgrade_is_role_terminal(net_sidecar) -> None:
    """Port of testUpgradeException421CarriesRoleHeader: a single-replica
    off-mode walk surfaces a role-reject terminal carrying the advertised role,
    not a generic socket error."""
    with FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as replica:
        with pytest.raises(RuntimeError) as ei:
            net_sidecar.connect(_reject_connect_string(replica.port))
        assert replica.connections >= 1, "replica endpoint must be probed"
    msg = str(ei.value)
    assert "REPLICA" in msg and "role=" in msg, msg


@pytest.mark.net_client
def test_all_replicas_upgrade_is_role_terminal(net_sidecar) -> None:
    """Port of testRoleMismatchExceptionWhenAllReplicas: every address is a
    non-writable role (REPLICA + PRIMARY_CATCHUP); the blocking walk probes them
    all, then surfaces a role-reject terminal (distinguishable from all-down)."""
    with FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as r1, \
            FakeUpgradeServer(421, "Misdirected Request", role="PRIMARY_CATCHUP") as r2:
        with pytest.raises(RuntimeError) as ei:
            net_sidecar.connect(_reject_connect_string(r1.port, r2.port))
        assert r1.connections >= 1 and r2.connections >= 1, "both endpoints must be probed"
    msg = str(ei.value)
    assert "REPLICA" in msg or "PRIMARY_CATCHUP" in msg, msg


@pytest.mark.net_client
def test_mixed_sweep_role_reject_outranks_terminal_503(net_sidecar) -> None:
    """Port of testMixedSweepRoleRejectOutranksLatchedTerminalUpgradeError:
    a sweep of [replica, replica, 503] must classify the exhausted round by the
    co-occurring role rejects (transient failover window), not the latched 503
    terminal — otherwise a transient window becomes a dead sender."""
    with FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as r1, \
            FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as r2, \
            FakeUpgradeServer(503, "Service Unavailable") as sick:
        with pytest.raises(RuntimeError) as ei:
            net_sidecar.connect(_reject_connect_string(r1.port, r2.port, sick.port))
    msg = str(ei.value)
    assert "REPLICA" in msg, f"role reject must outrank the 503 terminal: {msg}"


@pytest.mark.net_client
@pytest.mark.parametrize("status,reason", [(401, "Unauthorized"), (403, "Forbidden")])
def test_auth_denied_upgrade_is_terminal(net_sidecar, status: int, reason: str) -> None:
    """A writable node that denies the upgrade with 401/403 is an immediate
    terminal (AuthError) in any connect mode — no retry, no role-reject
    reclassification (handoff SECURITY_ERROR / ACL scenario, client side)."""
    with FakeUpgradeServer(status, reason) as srv:
        with pytest.raises(RuntimeError) as ei:
            net_sidecar.connect(f"ws::addr=127.0.0.1:{srv.port};username=admin;password=quest;")
        assert srv.connections == 1, "auth terminal must not retry the endpoint"
    msg = str(ei.value)
    assert "Auth" in msg and str(status) in msg, msg


@pytest.mark.net_client
def test_offmode_all_replica_window_retries_forever(net_sidecar) -> None:
    """Invariant B: an all-replica window is transient, not terminal. With an
    async (non-blocking) connect the sender keeps walking the replica forever —
    reconnect attempts climb and nothing is ever delivered or latched terminal —
    so a primary that later appears would be picked up (the delivery half of that
    needs a real promotion and is exercised in the Enterprise cluster suite)."""
    with FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as replica:
        net_sidecar.connect(
            f"ws::addr=127.0.0.1:{replica.port};initial_connect_retry=async;")
        net_sidecar.send("net_invariant_b", count=5, start_index=0)

        time.sleep(2.0)
        s1 = net_sidecar.stats()
        time.sleep(1.5)
        s2 = net_sidecar.stats()

    assert s1.reconn_attempts >= 1, f"expected reconnect attempts to accrue: {s1}"
    assert s2.reconn_attempts > s1.reconn_attempts, (
        f"all-replica window must keep retrying, not go terminal: {s1} -> {s2}")
    assert s2.acked == -1, f"nothing must be delivered while no primary exists: {s2}"


@pytest.mark.net_client
def test_failover_past_replica_to_primary(server_factory, net_sidecar,
                                          obj_store: ObjStore, scenario_dir: Path) -> None:
    """Port of testFailoverPastReplicaToPrimary: the address list leads with a
    rejecting replica and ends with a real primary. The off-mode walk rotates
    past the 421 replica within the round and lands on the primary, so every row
    is delivered."""
    table = "net_walk_to_primary"
    row_count = 30
    sf_dir = scenario_dir / "sf"

    primary = server_factory("p1")
    ports = primary.start()

    with FakeUpgradeServer(421, "Misdirected Request", role="REPLICA") as replica:
        cfg = (f"ws::addr=127.0.0.1:{replica.port},127.0.0.1:{ports.http}"
               f";user=admin;password=quest;sf_dir={sf_dir}"
               f";reconnect_max_duration_millis=30000;close_flush_timeout_millis=5000;")
        net_sidecar.connect(cfg)
        net_sidecar.send(table, count=row_count, start_index=0)
        net_sidecar.flush()
        assert replica.connections >= 1, "the rejecting replica must be probed before the primary"

    wait_for_dense_sequence(port=ports.pg, table=table,
                            expected_count=row_count, timeout_s=60.0)


# ---------------------------------------------------------------------------
# Real-cluster role-transition scenarios, ported from the Enterprise reference
# suite (questdb-ent/e2e/tests/test_demotion_mid_stream.py and
# test_durable_ack_failover.py) to drive the .NET sidecar instead of the Java
# one. These reuse the existing Enterprise fixtures unchanged: server_factory
# (role="primary"|"replica"), the min-http lifecycle control plane (lib.lifecycle
# submit_switch / await_role), and pg_query convergence probes. No Enterprise-side
# change is needed — the .NET sidecar has the same CONNECT/SEND/FLUSH/AWAIT_ACKED/
# STATS verbs as the Java one.
# ---------------------------------------------------------------------------

_CLUSTER_TABLE = "net_role_transition"
_CLUSTER_INITIAL_ROWS = 30
_CLUSTER_WINDOW_ROWS = 40
_CLUSTER_POST_ROWS = 20
_CLUSTER_INGEST_BATCH = 10
_CLUSTER_INGEST_BATCH_INTERVAL_S = 0.2
_CLUSTER_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000
_CLUSTER_AWAIT_ROLE_TIMEOUT_S = 60.0
_CLUSTER_POLL_INTERVAL_S = 0.25


def _cluster_connect_string(a_http: int, b_http: int, sf_dir: Path) -> str:
    """HA durable-ack connect string listing both endpoints (A primary, B replica).
    reconnect_max_duration_millis bounds only a blocking initial connect; the
    mid-stream reconnect loop that rides out the role transition never consults it
    (Invariant B — only SF exhaustion or a non-retriable reject is terminal)."""
    return (
        f"ws::addr=127.0.0.1:{a_http},127.0.0.1:{b_http}"
        ";user=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=300000"
        ";reconnect_initial_backoff_millis=100"
        ";reconnect_max_backoff_millis=1000"
        ";close_flush_timeout_millis=5000;"
    )


def _cluster_ingest_unaware(net_sidecar, *, count: int, start_index: int) -> int:
    """Produce rows exactly as a real SF producer does — UNAWARE of the role
    change: append (SEND) and publish to on-disk SF (FLUSH), both local ops that
    return whether or not a primary is reachable. The only terminal condition is
    SF exhaustion; a hard SEND/FLUSH failure across the transition IS the
    regression (e.g. the demoted node NACKing instead of sending a reconnect-
    eligible role-change close), so surface it as a descriptive assertion."""
    try:
        net_sidecar.send(_CLUSTER_TABLE, count=count, start_index=start_index)
        return net_sidecar.flush()
    except RuntimeError as e:  # NetSidecarError is a RuntimeError
        raise AssertionError(
            f"store-and-forward producer hard-failed while ingesting rows "
            f"[{start_index}..{start_index + count}) across a role transition — a "
            f"graceful role change must surface to the sender as a reconnect-eligible "
            f"close (retry from SF), never a terminal; only SF exhaustion may be "
            f"terminal. sidecar error: {e!r}")


def _cluster_ingest_range(net_sidecar, *, start_index: int, total: int) -> int:
    """Drive ``total`` rows in small batches with a brief pause so the producer is
    genuinely mid-stream across the demote/promote events; return the highest fsn."""
    end = start_index + total
    idx = start_index
    last_fsn = -1
    while idx < end:
        n = min(_CLUSTER_INGEST_BATCH, end - idx)
        last_fsn = _cluster_ingest_unaware(net_sidecar, count=n, start_index=idx)
        idx += n
        time.sleep(_CLUSTER_INGEST_BATCH_INTERVAL_S)
    return last_fsn


def _cluster_wait_count(*, port: int, expected: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last = -1
    while time.monotonic() < deadline:
        last = count_rows(port=port, table=_CLUSTER_TABLE)
        if last >= expected:
            return
        time.sleep(_CLUSTER_POLL_INTERVAL_S)
    raise AssertionError(
        f"row count on :{port} reached {last}, expected >= {expected} within {timeout_s}s")


def _cluster_await_all_replica_round(net_sidecar, baseline, *, timeout_s: float) -> None:
    """Coverage guard (counters only, never drives the producer): block until the
    sender COMPLETED a full reconnect round against the all-replica topology.
    reconnAttempts increments at the top of each round, so +2 proves the first
    round's walk finished (looped back and incremented again) — i.e. both nodes
    were reached and role-rejected — avoiding a promote-too-early race."""
    target = baseline.reconn_attempts + 2
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if net_sidecar.stats().reconn_attempts >= target:
            return
        time.sleep(0.1)
    raise AssertionError(
        "sender did not complete a full all-replica reconnect round "
        f"(needed reconnAttempts >= {target}); the mid-stream role-change close "
        "either never happened or was not treated as reconnect-eligible")


def _cluster_await_read_only_evidence(log_dir: Path, *, node: str, timeout_s: float) -> None:
    """Coverage witness: prove the demoted node's read-only gate fired on a
    mid-stream frame (the role-change-close trigger), so the green path is
    provably the one under test. The witness is the read-only refusal the forked
    server logs when its ingress rejects the in-flight frame."""
    needle = "replica access is read-only"
    files = (log_dir / f"{node}.stdout.log", log_dir / f"{node}.stderr.log")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for f in files:
            try:
                if needle in f.read_text(encoding="utf-8", errors="replace"):
                    return
            except FileNotFoundError:
                continue
        time.sleep(_CLUSTER_POLL_INTERVAL_S)
    raise AssertionError(
        f"no mid-stream read-only refusal in {node}'s logs within {timeout_s}s — the "
        f"role-change-close path this test exists to cover was not exercised")


@pytest.mark.net_client
def test_graceful_demotion_mid_stream_sender_survives(
        server_factory, net_sidecar, scenario_dir: Path, log_dir: Path) -> None:
    """Port of test_demotion_mid_stream: an in-place PRIMARY->REPLICA demote under
    a producing durable-ack sender surfaces as a reconnect-eligible role-change
    close (never a NACK); the sender rides the all-replica window and, once B is
    promoted, every row — including the frame rejected at the demote — lands
    exactly once."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert a_ports.min_http is not None, "node a: min_http port not reported (needed to demote A)"
    assert b_ports.min_http is not None, "node b: min_http port not reported (needed to promote B)"

    net_sidecar.connect(_cluster_connect_string(a_ports.http, b_ports.http, sf_dir))

    initial_fsn = _cluster_ingest_range(net_sidecar, start_index=0, total=_CLUSTER_INITIAL_ROWS)
    assert net_sidecar.await_acked(initial_fsn, _CLUSTER_DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A [publishedFsn={initial_fsn}]")
    wait_for_dense_sequence(port=a_ports.pg, table=_CLUSTER_TABLE,
                            expected_count=_CLUSTER_INITIAL_ROWS, timeout_s=60.0)
    _cluster_wait_count(port=b_ports.pg, expected=_CLUSTER_INITIAL_ROWS, timeout_s=120.0)

    baseline = net_sidecar.stats()
    assert baseline.server_errors == 0, (
        f"pre-demote baseline already carries server errors ({baseline.server_errors})")

    # Demote A in place (wait=False) so frames are in flight while the role flips.
    lc.submit_switch(a_ports.min_http, "replica", wait=False)
    _cluster_ingest_range(net_sidecar, start_index=_CLUSTER_INITIAL_ROWS,
                          total=_CLUSTER_WINDOW_ROWS // 2)
    lc.await_role(a_ports.min_http, "replica", timeout_s=_CLUSTER_AWAIT_ROLE_TIMEOUT_S)
    _cluster_ingest_range(net_sidecar,
                          start_index=_CLUSTER_INITIAL_ROWS + _CLUSTER_WINDOW_ROWS // 2,
                          total=_CLUSTER_WINDOW_ROWS - _CLUSTER_WINDOW_ROWS // 2)

    _cluster_await_all_replica_round(net_sidecar, baseline, timeout_s=30.0)
    _cluster_await_read_only_evidence(log_dir, node="a", timeout_s=15.0)

    # Wire-contract pin: the demote surfaced as a role-change close, not a NACK.
    stats = net_sidecar.stats()
    assert stats.server_errors == 0, (
        f"the in-place demote surfaced as {stats.server_errors} client-visible NACK(s); "
        f"a graceful role change must close with a reconnect-eligible NORMAL_CLOSURE")

    lc.submit_switch(b_ports.min_http, "primary", wait=True,
                     wait_timeout_s=_CLUSTER_AWAIT_ROLE_TIMEOUT_S)

    final_fsn = _cluster_ingest_range(
        net_sidecar, start_index=_CLUSTER_INITIAL_ROWS + _CLUSTER_WINDOW_ROWS,
        total=_CLUSTER_POST_ROWS)
    assert net_sidecar.await_acked(final_fsn, _CLUSTER_DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"rows produced across the in-place demote were lost [publishedFsn={final_fsn}]; "
        f"the frame rejected by the role-change close must replay from SF after reconnect")

    total = _CLUSTER_INITIAL_ROWS + _CLUSTER_WINDOW_ROWS + _CLUSTER_POST_ROWS
    # Dense = no loss AND no duplicates: the in-flight frame replays exactly once.
    wait_for_dense_sequence(port=b_ports.pg, table=_CLUSTER_TABLE,
                            expected_count=total, timeout_s=120.0)


@pytest.mark.net_client
def test_durable_ack_sender_survives_replica_only_window(
        server_factory, net_sidecar, scenario_dir: Path) -> None:
    """Port of test_durable_ack_failover: kill the primary so only a REPLICA is
    reachable; the durable-ack sender keeps buffering to SF through the all-replica
    window (Invariant B — never terminal), and once B is promoted every outage-
    window row drains and durably acks with no loss or duplication."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert b_ports.min_http is not None, "node b: min_http port not reported (needed to promote B)"

    net_sidecar.connect(_cluster_connect_string(a_ports.http, b_ports.http, sf_dir))

    initial_fsn = _cluster_ingest_range(net_sidecar, start_index=0, total=_CLUSTER_INITIAL_ROWS)
    assert net_sidecar.await_acked(initial_fsn, _CLUSTER_DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A [publishedFsn={initial_fsn}]")
    wait_for_dense_sequence(port=a_ports.pg, table=_CLUSTER_TABLE,
                            expected_count=_CLUSTER_INITIAL_ROWS, timeout_s=60.0)
    _cluster_wait_count(port=b_ports.pg, expected=_CLUSTER_INITIAL_ROWS, timeout_s=120.0)

    baseline = net_sidecar.stats()
    a.kill_9()

    # Produce straight through the replica-only window; rows accumulate in SF.
    _cluster_ingest_range(net_sidecar, start_index=_CLUSTER_INITIAL_ROWS,
                          total=_CLUSTER_WINDOW_ROWS)
    _cluster_await_all_replica_round(net_sidecar, baseline, timeout_s=30.0)

    lc.submit_switch(b_ports.min_http, "primary", wait=True,
                     wait_timeout_s=_CLUSTER_AWAIT_ROLE_TIMEOUT_S)

    final_fsn = _cluster_ingest_range(
        net_sidecar, start_index=_CLUSTER_INITIAL_ROWS + _CLUSTER_WINDOW_ROWS,
        total=_CLUSTER_POST_ROWS)
    assert net_sidecar.await_acked(final_fsn, _CLUSTER_DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"rows produced across the failover were lost [publishedFsn={final_fsn}]; an SF "
        f"sender must retain outage-window rows and drain them after promotion")

    total = _CLUSTER_INITIAL_ROWS + _CLUSTER_WINDOW_ROWS + _CLUSTER_POST_ROWS
    wait_for_dense_sequence(port=b_ports.pg, table=_CLUSTER_TABLE,
                            expected_count=total, timeout_s=120.0)


# Still deferred (need Enterprise fixture capabilities not present today):
#   * SECURITY_ERROR / ACL denial on a writable node — needs per-user ACL provisioning
#     in server_factory.
#   * durable-ack capability gap terminal — needs a node started without replication so
#     the /write/v4 upgrade omits the durable-ack header.
# The PrReviewRedTestsE2e C4/C11 and promoted-replica-stickiness / standalone-writable
# cases stay covered by the .NET unit suite (redundant to re-drive through the sidecar).
