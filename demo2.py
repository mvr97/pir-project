import time
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def print_db_snapshot(db_array, highlight_indices=None):
    """Utility helper to print a formatted snapshot of database slots."""
    if highlight_indices is None:
        highlight_indices = []
    
    formatted_slots = []
    for idx in range(10):  # Showing first 10 slots for clean terminal view
        val = db_array[idx]
        if idx in highlight_indices:
            formatted_slots.append(f"[{idx:02d}]: \033[92m{val:3d}\033[0m")  # Highlight green
        else:
            formatted_slots.append(f"[{idx:02d}]: {val:3d}")
    print("      " + " | ".join(formatted_slots[:5]))
    print("      " + " | ".join(formatted_slots[5:10]))

def main():
    print("=" * 80)
    print("   DEMO 2: DATABASE STATE MUTATIONS & EPOCH COMPACTION VISUALIZER")
    print("=" * 80)

    DB_SIZE = 100
    BLOCK_SIZE = 10
    NUM_HINTS = 200  # enough hints that every (block, offset) is covered by at least one hint

    # 1. Initialize Server & Client
    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)

    print("\n[STEP 1] INITIAL DATABASE STATE ON SERVER (First 10 Slots):")
    print_db_snapshot(server.db)

    print("\n[STEP 2] CLIENT PREPROCESSING:")
    client.offline_preprocessing(server.db)
    print(" Client streamed database once and calculated offline hint parities.\n")

    # Save initial values of target indices for comparison tracking
    initial_idx_2 = server.db[2]
    initial_idx_5 = server.db[5]

    print("=" * 80)
    print(" [STEP 3] EXECUTING REAL-TIME DATABASE MUTATIONS (EPOCH #1)")
    print("=" * 80)
    print(" Scenario: Rapid updates occurring on Index 02 and Index 05...\n")

    # Sequence of rapid mutations
    mutations = [
        (2, 45,  "Index 02 updated to 45"),
        (5, 120, "Index 05 updated to 120"),
        (2, 88,  "Index 02 overwritten again to 88"),
        (2, 210, "Index 02 overwritten third time to 210 (Final state for Index 02)"),
        (5, 199, "Index 05 overwritten again to 199 (Final state for Index 05)")
    ]

    for step_num, (idx, new_val, description) in enumerate(mutations, 1):
        old_val = server.db[idx]
        server.receive_update(idx, new_val)
        
        print(f" Mutation #{step_num}: {description}")
        print(f"   -> Version Log Entry added: t={server.current_version:02d} | Slot [{idx:02d}] transitioned {old_val:3d} ---> {new_val:3d}")
        print("   -> Server DB Snapshot:")
        print_db_snapshot(server.db, highlight_indices=[idx])
        print()

    print("=" * 80)
    print(" [STEP 4] RAW UNCOMPACTED VERSION LOG BUFFER")
    print("=" * 80)
    print(f" {'Ver (t)':<8} | {'Slot Index':<12} | {'Old Value':<12} | {'New Value':<12} | {'State Action'}")
    print("-" * 75)
    for entry in server.version_log:
        print(f" v{entry['version']:<7} | Slot [{entry['index']:02d}]     | {entry['old_val']:<12} | {entry['new_val']:<12} | Logged to Buffer")
    print("-" * 75)

    print("=" * 80)
    print(" [STEP 5] EPOCH COMPACTION & NET DELTA PATCH GENERATION")
    print("=" * 80)
    print(" Server executing end_epoch_and_compact()...")
    print(" Processing log: Intermediate mutations (A -> B -> C) are discarded.")
    print(" Only net change (Initial_Pre_Epoch ^ Final_Post_Epoch) is extracted.\n")

    compacted_deltas = server.end_epoch_and_compact()

    print(" COMPACTED PATCH TRANSMITTED TO CLIENT:")
    print("-" * 80)
    print(f" {'Slot Index':<12} | {'Initial Val':<14} | {'Final Val':<12} | {'Net XOR Delta (Δ)':<20}")
    print("-" * 80)
    
    print(f" Slot [02]     | {initial_idx_2:<14} | {server.db[2]:<12} | {compacted_deltas[2]:<20}")
    print(f" Slot [05]     | {initial_idx_5:<14} | {server.db[5]:<12} | {compacted_deltas[5]:<20}")
    print("-" * 80)
    print(f" Summary: 5 raw version updates compacted down to {len(compacted_deltas)} net deltas!\n")

    print("=" * 80)
    print(" [STEP 6] CLIENT APPLYING COMPACTED PATCH TO HINTS")
    print("=" * 80)
    updated_hints = client.apply_epoch_patch(compacted_deltas)
    print(f" Client applied net deltas to {updated_hints} affected hints using O(1) iPRF inversion.")
    print(" Client hints are now 100% synchronized with Server Database.\n")

    print("=" * 80)
    print(" [STEP 7] PIR QUERY VERIFICATION POST-COMPACTION")
    print("=" * 80)
    
    # Query Index 2
    retrieved_2, _ = client.query(2, server)
    actual_2 = server.db[2]
    print(f" PIR Query for Slot [02]: Retrieved = {retrieved_2:3d} | Actual DB = {actual_2:3d} | Match = {retrieved_2 == actual_2}")

    # Query Index 5
    retrieved_5, _ = client.query(5, server)
    actual_5 = server.db[5]
    print(f" PIR Query for Slot [05]: Retrieved = {retrieved_5:3d} | Actual DB = {actual_5:3d} | Match = {retrieved_5 == actual_5}")
    print("=" * 80)

if __name__ == "__main__":
    main()
