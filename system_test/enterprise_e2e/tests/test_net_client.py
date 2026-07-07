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

from lib.obj_store import ObjStore
from lib.pg_query import wait_for_dense_sequence
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


# Intentionally NOT ported here:
#   * Java testFailoverPromotedReplicaJoinsRotation / testStandaloneIsTreatedAsWritable
#     and PrReviewRedTestsE2e C4 (terminal latched before handler) / C11 (post-halt
#     flush throws typed) are pure client-behaviour cases already covered by the .NET
#     unit suite (QwpRoleFilterTests, QwpCursorSendEngineMultiHostTests, and the WP1/WP5
#     terminal-error tests). Re-driving them through the sidecar would be redundant.
#   * The *delivery* half of the all-replica window (retry forever, then land every row
#     once a node is promoted to PRIMARY) and graceful primary->replica demotion need a
#     real role transition from server_factory; the durable-ack capability gap needs a
#     primary without replication configured. Those depend on Enterprise fixture
#     capabilities and belong in the cluster suite once the fixtures expose promotion /
#     demotion / no-replication start modes.
