import time
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def main():
    print("=" * 70)
    print(" PLINKO PIR: BENCHMARKING IMPROVEMENT 2 (EPOCH BATCHED UPDATES)")
    print("=" * 70)

    DB_SIZE = 5000
    BLOCK_SIZE = 20
    NUM_HINTS = 500

    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)

    print(" [Setup] Preprocessing offline hints...")
    client.offline_preprocessing(server.db)
    print(" [Setup] Complete.\n")

    # -------------------------------------------------------------
    # SIMULATION WORKLOAD: High-Frequency Updates (e.g., Live Scores)
    # -------------------------------------------------------------
    # Imagine 1,000 rapid updates happening across 50 indices
    # (some indices updated 20+ times during the epoch!)
    NUM_UPDATES = 1000
    UPDATED_INDICES_POOL = [100, 250, 427, 890, 1200, 2500, 3100, 4500]  # Frequently updated hot indices
    
    print(f"--- SIMULATING WORKLOAD: {NUM_UPDATES} Database Updates ---")
    
    # -------------------------------------------------------------
    # APPROACH A: Baseline Plinko (Individual / Unbatched Updates)
    # -------------------------------------------------------------
    t0_unbatched = time.time()
    unbatched_hint_updates = 0
    
    for i in range(NUM_UPDATES):
        idx = UPDATED_INDICES_POOL[i % len(UPDATED_INDICES_POOL)]
        new_val = (i * 7) % 256
        
        # In baseline unbatched PIR, server immediately pushes delta for every update
        old_val = server.db[idx]
        delta = old_val ^ new_val
        server.db[idx] = new_val  # Server updates DB
        
        # Client updates hint for every single write
        b = idx // BLOCK_SIZE
        off = idx % BLOCK_SIZE
        affected = iprf.invert(b, off)
        for h_id in affected:
            if idx in client.hints[h_id]['indices']:
                client.hints[h_id]['parity'] ^= delta
                unbatched_hint_updates += 1
                
    time_unbatched = time.time() - t0_unbatched
    network_messages_unbatched = NUM_UPDATES  # 1 message per update

    # Reset DB state for clean epoch test
    server = PlinkoServer(DB_SIZE)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)
    client.offline_preprocessing(server.db)

    # -------------------------------------------------------------
    # APPROACH B: Improvement 2 (Epoch-Based Batched Updates)
    # -------------------------------------------------------------
    t0_batched = time.time()
    
    # 1. Server logs updates during epoch
    for i in range(NUM_UPDATES):
        idx = UPDATED_INDICES_POOL[i % len(UPDATED_INDICES_POOL)]
        new_val = (i * 7) % 256
        server.receive_update(idx, new_val)
        
    # 2. End of Epoch: Server compacts intermediate updates
    compacted_deltas = server.end_epoch_and_compact()
    
    # 3. Client applies patch in a single batch pass
    batched_hint_updates = client.apply_epoch_patch(compacted_deltas)
    
    time_batched = time.time() - t0_batched
    network_messages_batched = len(compacted_deltas)  # Only 1 message per unique modified entry!

    # -------------------------------------------------------------
    # VERIFICATION & COMPARISON METRICS
    # -------------------------------------------------------------
    print("\n" + "=" * 70)
    print("                    PERFORMANCE IMPROVEMENT RESULTS")
    print("=" * 70)
    print(f" Workload: {NUM_UPDATES} raw database writes occurring in 1 Epoch.\n")
    
    print(f" 1. Network Communication (Server -> Client Update Messages):")
    print(f"    - Baseline (Unbatched):  {network_messages_unbatched} messages")
    print(f"    - Improvement 2 (Epoch):  {network_messages_batched} messages")
    reduction_net = ((network_messages_unbatched - network_messages_batched) / network_messages_unbatched) * 100
    print(f"    --> Network Traffic Reduced by: {reduction_net:.1f}%\n")
    
    print(f" 2. Client Computation (Total Hint Parity Modifications):")
    print(f"    - Baseline (Unbatched):  {unbatched_hint_updates} operations")
    print(f"    - Improvement 2 (Epoch):  {batched_hint_updates} operations")
    reduction_comp = ((unbatched_hint_updates - batched_hint_updates) / unbatched_hint_updates) * 100
    print(f"    --> Client Hint Re-computations Reduced by: {reduction_comp:.1f}%\n")

    print(f" 3. Total Synchronization Processing Time:")
    print(f"    - Baseline (Unbatched):  {time_unbatched:.4f} seconds")
    print(f"    - Improvement 2 (Epoch):  {time_batched:.4f} seconds")
    speedup = time_unbatched / time_batched if time_batched > 0 else 1.0
    print(f"    --> Speedup Factor: {speedup:.2f}x Faster!")
    print("=" * 70)

if __name__ == "__main__":
    main()