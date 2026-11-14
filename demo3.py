import pprint
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def print_log_state(title, version_log):
    """Helper function to print a clean, detailed view of the raw version_log array."""
    print()
    print(f" {title}")
    print()
    print(f" Current `server.version_log` length: {len(version_log)} entries")
    if not version_log:
        print(" [LOG STATUS]: Empty `[]` (No pending uncompacted mutations)\n")
        return

    print("-" * 80)
    print(f" {'Log Index':<10} | {'Ver (t)':<8} | {'DB Slot':<10} | {'Old Value':<12} | {'New Value':<12}")
    print("-" * 80)
    for i, entry in enumerate(version_log):
        print(f" Entry [{i}]   | v{entry['version']:<7} | Slot [{entry['index']:02d}]   | {entry['old_val']:<12} | {entry['new_val']:<12}")
    print("-" * 80 + "\n")


def main():
    print()
    print("*** DEMO 3: VERSION LOG CONTENT INSPECTOR ***")
    print()

    DB_SIZE = 100
    BLOCK_SIZE = 10
    NUM_HINTS = 200  # enough hints that every (block, offset) is covered

    # 1. Initialize Server & Client
    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)

    print("\n[SETUP] Client streaming DB once offline to compute hints...")
    client.offline_preprocessing(server.db)
    print("[SETUP] Preprocessing Complete.\n")

    # PHASE 1: PRE-MUTATION LOG INSPECTION
    print_log_state("PHASE 1: INSPECTING LOG BEFORE ANY DATABASE MUTATIONS", server.version_log)

    # PHASE 2: MUTATION-BY-MUTATION LOG BUILDING
    print("PHASE 2: EXECUTING MUTATIONS & TRACKING `version_log` GROWTH")
    print()

    mutations = [
        (10, 50,  "Write #1: Slot [10] updated to 50"),
        (25, 110, "Write #2: Slot [25] updated to 110"),
        (10, 88,  "Write #3: Slot [10] overwritten to 88"),
        (10, 240, "Write #4: Slot [10] overwritten again to 240"),
        (42, 199, "Write #5: Slot [42] updated to 199"),
        (25, 77,  "Write #6: Slot [25] overwritten again to 77")
    ]

    for step, (idx, val, desc) in enumerate(mutations, 1):
        server.receive_update(idx, val)
        print(f" >>> Executed {desc}")
        print(f"     Appended Entry: {server.version_log[-1]}")
        print(f"     Total log entries so far: {len(server.version_log)}\n")

    # PHASE 3: RAW LOG BUFFER BEFORE COMPACTION
    print_log_state("PHASE 3: RAW UNCOMPACTED `server.version_log` BUFFER AT EPOCH END", server.version_log)

    # PHASE 4: COMPACTION & EXTRACTION OF NET DELTAS
    print()
    print("PHASE 4: COMPACTING `version_log` INTO NET DELTAS")
    print()
    print(" Server calling `server.end_epoch_and_compact()`...")
    print(" Discarding intermediate state transitions (A -> B -> C => A -> C)...")

    compacted_deltas = server.end_epoch_and_compact()

    print("\n GENERATED COMPACTED NET DELTAS (Sent over the wire to client):")
    print("-" * 80)
    print(f" {'DB Slot Index':<15} | {'Net XOR Delta (Δ)':<25} | {'Explanation'}")
    print("-" * 80)
    for idx, delta in compacted_deltas.items():
        print(f" Slot [{idx:02d}]          | {delta:<25} | Net change across epoch")
    print("-" * 80 + "\n")

    # PHASE 5: POST-COMPACTION LOG INSPECTION
    print_log_state("PHASE 5: INSPECTING `server.version_log` AFTER COMPACTION", server.version_log)

    # PHASE 6: CLIENT APPLYING PATCH & VERIFYING QUERY
    print()
    print(" PHASE 6: CLIENT HINT SYNCHRONIZATION & PIR QUERY VERIFICATION")
    print()
    applied_hints = client.apply_epoch_patch(compacted_deltas)
    print(f" Client updated {applied_hints} hint parities using iPRF inversion.")

    target_test = 10
    retrieved, _ = client.query(target_test, server)
    actual = server.db[target_test]
    print(f" PIR Query for Slot [10]: Retrieved = {retrieved} | Actual DB = {actual} | Match = {retrieved == actual}")
    print()

if __name__ == "__main__":
    main()
