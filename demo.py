import time
import random
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def run_benchmark():
    print("=" * 80)
    print("   DEMO 1: PERFORMANCE BENCHMARK (BASELINE VS IMPROVEMENT 2)")
    print("=" * 80)

    DB_SIZE = 1000
    BLOCK_SIZE = 10
    NUM_HINTS = 200
    TOTAL_WRITES = 1000
    HOT_INDICES_COUNT = 8  # Write contention concentrated on 8 hot slots

    # 1. Setup Server & Client
    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)

    print(" [Setup] Streaming DB once for offline hint precomputation...")
    client.offline_preprocessing(server.db)
    print(" [Setup] Setup Complete!\n")

    hot_indices = random.sample(range(DB_SIZE), HOT_INDICES_COUNT)
    print(f" Simulating {TOTAL_WRITES} rapid database writes focused on {HOT_INDICES_COUNT} hot slots...")

    # --- SIMULATING BASELINE PLINKO (Item-by-item updates) ---
    baseline_messages = TOTAL_WRITES
    baseline_client_ops = 0
    start_time_base = time.time()
    
    for _ in range(TOTAL_WRITES):
        idx = random.choice(hot_indices)
        val = random.randint(1, 255)
        old_val = server.db[idx]
        server.db[idx] = val
        delta = old_val ^ val
        # Baseline calls iPRF invert and updates hints per write
        b = idx // BLOCK_SIZE
        off = idx % BLOCK_SIZE
        hints_to_update = iprf.invert(b, off)
        baseline_client_ops += len(hints_to_update)
        
    time_base = (time.time() - start_time_base) * 1000  # ms

    # --- SIMULATING IMPROVEMENT 2 (Epoch-Based Patch Compaction) ---
    server_imp2 = PlinkoServer(DB_SIZE)
    client_imp2 = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)
    client_imp2.offline_preprocessing(server_imp2.db)

    start_time_imp2 = time.time()
    
    # 1. Buffer writes into Version Log during Epoch
    for _ in range(TOTAL_WRITES):
        idx = random.choice(hot_indices)
        val = random.randint(1, 255)
        server_imp2.receive_update(idx, val)

    # 2. End Epoch & Compact
    compacted_deltas = server_imp2.end_epoch_and_compact()
    imp2_messages = len(compacted_deltas)

    # 3. Single-Pass Hint Synchronization
    imp2_client_ops = client_imp2.apply_epoch_patch(compacted_deltas, server_db=server_imp2.db)
    time_imp2 = (time.time() - start_time_imp2) * 1000  # ms

    # --- BENCHMARK RESULTS ---
    print("\n" + "=" * 80)
    print(" BENCHMARK RESULTS SUMMARY")
    print("=" * 80)
    print(f" {'Metric':<30} | {'Baseline Plinko':<18} | {'Improvement 2 (Ours)':<20} | {'Savings / Improvement'}")
    print("-" * 80)
    
    msg_savings = ((baseline_messages - imp2_messages) / baseline_messages) * 100
    ops_savings = ((baseline_client_ops - imp2_client_ops) / baseline_client_ops) * 100
    
    print(f" {'Network Messages Sent':<30} | {baseline_messages:<18} | {imp2_messages:<20} | {msg_savings:.2f}% Reduction")
    print(f" {'Client Hint XOR Updates':<30} | {baseline_client_ops:<18} | {imp2_client_ops:<20} | {ops_savings:.2f}% Reduction")
    print(f" {'Sync Time (ms)':<30} | {time_base:<18.2f} | {time_imp2:<20.2f} | {(time_base/max(0.001, time_imp2)):.1f}x Faster")
    print("-" * 80)

if __name__ == "__main__":
    run_benchmark()
