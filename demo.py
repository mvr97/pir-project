import random
import time

from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient


def apply_one_baseline_update(client, iprf, index, delta):
    """Apply one update immediately, as in item-by-item synchronization."""
    block = index // client.w
    offset = index % client.w
    affected_hints = iprf.invert(block, offset)

    xor_updates = 0
    for hint_id in affected_hints:
        # Inversion returns candidates; only hints containing this index change.
        if block in client.hints[hint_id]["blocks_set"]:
            client.hints[hint_id]["parity"] ^= delta
            xor_updates += 1

    return xor_updates


def verify_queries(server, client, indices):
    """Check that synchronized hints reconstruct the current database values."""
    for index in indices:
        recovered, _ = client.query(index, server)
        if recovered != server.db[index]:
            raise AssertionError(
                f"Query failed at index {index}: got {recovered}, "
                f"expected {server.db[index]}"
            )


def run_benchmark():
    print()
    print("*** DEMO 1: BASELINE VS EPOCH-BASED PATCH COMPACTION ***")
    print()
    DB_SIZE = 1000
    BLOCK_SIZE = 10
    NUM_HINTS = 200
    TOTAL_WRITES = 1000
    HOT_INDICES_COUNT = 8
    SEED = 2026
    HINT_SEED = 12345

    if DB_SIZE % BLOCK_SIZE != 0:
        raise ValueError("DB_SIZE must be divisible by BLOCK_SIZE")

    # Generate one initial database and one update workload for BOTH systems.
    random.seed(SEED)
    initial_db = [random.randint(0, 255) for _ in range(DB_SIZE)]
    hot_indices = random.sample(range(DB_SIZE), HOT_INDICES_COUNT)
    updates = [
        (random.choice(hot_indices), random.randint(0, 255))
        for _ in range(TOTAL_WRITES)
    ]

    print(f"[Setup] Database size: {DB_SIZE}")
    print(f"[Setup] Writes: {TOTAL_WRITES}; hot indices: {HOT_INDICES_COUNT}")
    print("[Setup] Both approaches use the same database and exact same writes.")
    print()

    # -------------------- BASELINE --------------------
    baseline_server = PlinkoServer(DB_SIZE)
    baseline_server.db = initial_db.copy()

    baseline_iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    baseline_client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, baseline_iprf)
    random.seed(HINT_SEED)
    baseline_client.offline_preprocessing(baseline_server.db)

    baseline_hint_xors = 0
    baseline_start = time.perf_counter()

    for index, new_value in updates:
        old_value = baseline_server.db[index]
        delta = old_value ^ new_value
        baseline_server.db[index] = new_value
        baseline_hint_xors += apply_one_baseline_update(
            baseline_client, baseline_iprf, index, delta
        )

    baseline_time_ms = (time.perf_counter() - baseline_start) * 1000

    # ---------------- IMPROVEMENT 2 -------------------
    improved_server = PlinkoServer(DB_SIZE)
    improved_server.db = initial_db.copy()

    improved_iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    improved_client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, improved_iprf)
    random.seed(HINT_SEED)
    improved_client.offline_preprocessing(improved_server.db)

    improved_start = time.perf_counter()

    for index, new_value in updates:
        improved_server.receive_update(index, new_value)

    improved_server.end_epoch_and_compact()
    compacted_deltas = improved_server.get_patches_since_epoch(0)
    compacted_entries = len(compacted_deltas)
    improved_hint_xors = improved_client.apply_epoch_patch(
        compacted_deltas, server_db=improved_server.db
    )

    improved_time_ms = (time.perf_counter() - improved_start) * 1000

    # ---------------- CORRECTNESS CHECKS --------------
    if baseline_server.db != improved_server.db:
        raise AssertionError("Baseline and improved databases differ")

    # Check every hot index plus a reproducible sample of other indices.
    random.seed(SEED + 1)
    query_indices = sorted(set(hot_indices + random.sample(range(DB_SIZE), 25)))
    verify_queries(baseline_server, baseline_client, query_indices)
    verify_queries(improved_server, improved_client, query_indices)

    # -------------------- RESULTS ----------------------
    write_reduction = (1 - compacted_entries / TOTAL_WRITES) * 100
    xor_reduction = (
        (1 - improved_hint_xors / baseline_hint_xors) * 100
        if baseline_hint_xors else 0.0
    )
    speedup = (
        baseline_time_ms / improved_time_ms if improved_time_ms > 0 else float("inf")
    )

    print("*** BENCHMARK RESULTS ***")
    print("-" * 80)
    print(f"{'Metric':<38} | {'Baseline':>16} | {'Improvement 2':>18}")
    print("-" * 80)
    print(f"{'Database writes received':<38} | {TOTAL_WRITES:>16} | {TOTAL_WRITES:>18}")
    print(f"{'Update entries synchronized':<38} | {TOTAL_WRITES:>16} | {compacted_entries:>18}")
    print(f"{'Client hint XOR operations':<38} | {baseline_hint_xors:>16} | {improved_hint_xors:>18}")
    print(f"{'Synchronization time (ms)':<38} | {baseline_time_ms:>16.3f} | {improved_time_ms:>18.3f}")
    print("-" * 80)
    print()
    print(f"Compacted update-entry reduction: {write_reduction:.2f}%")
    print(f"Hint XOR-operation reduction:     {xor_reduction:.2f}%")
    print(f"Measured synchronization speedup: {speedup:.2f}x")
    print(f"Query correctness:                PASS ({len(query_indices)} indices per system)")
    print("Final database equality:           PASS")
    print()

if __name__ == "__main__":
    run_benchmark()
