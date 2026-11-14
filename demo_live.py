import time
import random
from iprf import SimpleIPRF
from server import PlinkoServer
from client import PlinkoClient

def simulate_live_database():
    print()
    print("*** LIVE SYSTEM DEMO: CONTINUOUS EPOCH-BASED PIR SYNCHRONIZATION ***")
    print()

    DB_SIZE = 1000
    BLOCK_SIZE = 10
    NUM_HINTS = 200
    EPOCH_INTERVAL_SECONDS = 1.5
    TOTAL_EPOCHS_TO_RUN = 3

    server = PlinkoServer(DB_SIZE)
    iprf = SimpleIPRF(BLOCK_SIZE, NUM_HINTS)
    client = PlinkoClient(DB_SIZE, BLOCK_SIZE, NUM_HINTS, iprf)

    print(" [Setup] Server DB initialized.")
    print(" [Setup] Client precomputing hints offline...")
    client.offline_preprocessing(server.db)
    print(" [Setup] Live System Ready!\n")

    HOT_INDICES = [42, 100, 250, 427, 890]

    for epoch_num in range(1, TOTAL_EPOCHS_TO_RUN + 1):
        print()
        print(f"*** STARTING LIVE EPOCH #{epoch_num} (Version t = {server.current_version}) ***")
        print()

        num_writes_this_epoch = random.randint(8, 15)
        print(f" [Live Stream] Incoming traffic: {num_writes_this_epoch} rapid writes flowing in...")

        for w in range(num_writes_this_epoch):
            target_idx = random.choice(HOT_INDICES)
            new_val = random.randint(1, 255)
            server.receive_update(target_idx, new_val)
            time.sleep(0.04)

        print(f" [Epoch Buffer] End of Epoch #{epoch_num} reached!")
        print(f" [Epoch Buffer] Raw Version Log Buffer contains {len(server.version_log)} uncompacted mutation entries.")

        compacted_deltas = server.end_epoch_and_compact()
        print(f" [Compaction] Server compressed {num_writes_this_epoch} raw writes into {len(compacted_deltas)} net deltas (Δ_{epoch_num}).")

        updated_hints = client.apply_epoch_patch(compacted_deltas, server_db=server.db)
        print(f" [Client Sync] Client updated {updated_hints} unique hint parities in a single pass.")

        test_idx = 427
        retrieved_val, _ = client.query(test_idx, server)
        actual_val = server.db[test_idx]
        print(f" [PIR Query Test] Re-query Slot [{test_idx}]: Client Retrieved = {retrieved_val:3d} | Actual DB = {actual_val:3d} | Correct = {retrieved_val == actual_val}\n")

        time.sleep(EPOCH_INTERVAL_SECONDS)

    print()
    print("*** LIVE CONTINUOUS SYSTEM SIMULATION COMPLETE ***")
    print()

if __name__ == "__main__":
    simulate_live_database()
