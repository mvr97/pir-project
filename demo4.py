import time
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def print_banner(title):
    print("\n" + "=" * 80)
    print(f"   {title}")
    print("=" * 80)

def main():
    print_banner("DEMO 4: BATCH-WINDOW EPOCH COMPACTION INSPECTOR")
    
    DB_SIZE = 1000
    BLOCK_SIZE = 10
    NUM_HINTS = 200
    
    # PARAMETERS FOR THIS DEMO
    TOTAL_MUTATIONS = 30
    EPOCH_BATCH_SIZE = 5  # Epoch triggers every 5 writes!
    
    # Initialize Server & Client
    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)
    
    print(" [Setup] Initializing Server Database...")
    print(" [Setup] Client streaming DB offline to precompute hints...")
    client.offline_preprocessing(server.db)
    print(" [Setup] Initialization Complete!\n")

    # Hot indices to demonstrate overlapping writes and compaction
    HOT_INDICES = [100, 250, 427, 890]
    
    # Generate 30 deterministic workload mutations
    workload = []
    for i in range(1, TOTAL_MUTATIONS + 1):
        idx = HOT_INDICES[i % len(HOT_INDICES)]
        val = (i * 17) % 256
        workload.append((idx, val))

    epoch_count = 0
    
    # Process workload 5 mutations at a time
    for i in range(0, TOTAL_MUTATIONS, EPOCH_BATCH_SIZE):
        epoch_count += 1
        batch = workload[i : i + EPOCH_BATCH_SIZE]
        
        print_banner(f"EPOCH #{epoch_count}: BUFFERING {len(batch)} INCOMING MUTATIONS")
        
        # 1. Simulate incoming writes into the Version Log
        for idx, new_val in batch:
            old_val = server.db[idx]
            server.receive_update(idx, new_val)
            print(f"  [Mutation Logged] Version t={server.current_version:02d} | Slot [{idx:03d}]: {old_val:3d} ---> {new_val:3d}")
        
        # 2. Display Raw Version Log state prior to compaction
        print("\n  --> RAW UNCOMPACTED VERSION LOG BUFFER (5 Entries):")
        print("      " + "-" * 55)
        print(f"      {'Version':<10} | {'Slot Index':<12} | {'Old Val':<10} | {'New Val':<10}")
        print("      " + "-" * 55)
        for entry in server.version_log:
            print(f"      v{entry['version']:<9} | Slot [{entry['index']:03d}]    | {entry['old_val']:<10} | {entry['new_val']:<10}")
        print("      " + "-" * 55)
        
        # 3. Trigger Epoch Compaction
        print(f"\n  [Trigger] Epoch #{epoch_count} Boundary Reached! Executing end_epoch_and_compact()...")
        compacted_deltas = server.end_epoch_and_compact()
        
        # 4. Show Compacted Net Deltas
        print("\n  --> COMPACTED NET DELTAS TRANSMITTED TO CLIENT (Δ_t):")
        print("      " + "-" * 60)
        print(f"      {'Slot Index':<12} | {'Net Delta (Δ)':<18} | {'Status'}")
        print("      " + "-" * 60)
        for idx, delta in compacted_deltas.items():
            print(f"      Slot [{idx:03d}]    | Δ = {delta:<14} | Compacted intermediate edits")
        print("      " + "-" * 60)
        print(f"  [Summary] {len(batch)} raw version log entries compressed into {len(compacted_deltas)} net deltas!")
        
        # 5. Client updates hints & executes a PIR query to verify correctness
        updated_hints = client.apply_epoch_patch(compacted_deltas)
        print(f"  [Client Sync] Applied net deltas to {updated_hints} hints in a single pass.")
        
        # Verification PIR Query on Slot 427
        test_idx = 427
        retrieved_val, _ = client.query(test_idx, server)
        actual_val = server.db[test_idx]
        print(f"  [PIR Query Check] Slot [{test_idx}]: Client Retrieved = {retrieved_val:3d} | Actual DB = {actual_val:3d} | Correct = {retrieved_val == actual_val}")
        
        time.sleep(0.5)  # Slight pause for terminal visual effect

    print_banner("DEMO 4 COMPLETE: ALL 30 MUTATIONS PROCESSED ACROSS 6 COMPACTED EPOCHS")

if __name__ == "__main__":
    main()
