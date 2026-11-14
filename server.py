import random

class PlinkoServer:
    """
    Server holding the raw database array.
    Implements a Versioned Update Log System and Patch Compaction
    as specified in Sections 3 and 5.2 of the Term Paper.
    """
    def __init__(self, db_size: int, epoch_threshold: int = 100):
        self.N = db_size
        self.db = [random.randint(0, 255) for _ in range(self.N)]
        
        # VERSION LOG SYSTEM
        self.current_version = 0
        self.version_log = []  # Stores active uncompacted mutations for current epoch
        
        # MULTI-EPOCH HISTORY (Optional: Allows offline clients to catch up across past epochs)
        self.epoch_counter = 0
        self.historical_epoch_patches = {}  # {epoch_id: compacted_deltas}
        
        # AUTOMATED TRIGGER PARAMETER
        self.epoch_threshold = epoch_threshold  # Auto-compacts if uncompacted writes reach threshold

    def answer_query(self, query_indices: list) -> int:
        """Computes XOR parity over requested indices without knowing target index."""
        parity = 0
        for idx in query_indices:
            parity ^= self.db[idx]
        return parity

    def receive_update(self, index: int, new_val: int) -> dict:
        """
        Logs database changes into an explicit Version Log.
        Increments current database version t -> t+1.
        Automatically triggers epoch compaction if buffer reaches epoch_threshold.
        Uses a zero-allocation fast-path when epoch_threshold == 1.
        """
        old_val = self.db[index]
        self.current_version += 1
        self.db[index] = new_val

        # FAST-PATH: Bypass version_log and compaction dictionary overhead for single-write epochs
        if self.epoch_threshold == 1:
            self.epoch_counter += 1
            delta = old_val ^ new_val
            compacted_deltas = {index: delta} if delta != 0 else {}
            self.historical_epoch_patches[self.epoch_counter] = compacted_deltas
            return compacted_deltas

        # Record versioned change in log for batched epochs
        self.version_log.append({
            'version': self.current_version,
            'index': index,
            'old_val': old_val,
            'new_val': new_val
        })

        # AUTOMATED EPOCH TRIGGER CHECK
        if len(self.version_log) >= self.epoch_threshold:
            return self.end_epoch_and_compact()
        return None

    def end_epoch_and_compact(self) -> dict:
        """
        Processes the Version Log for the epoch.
        Compacts multi-step updates for identical indices into net XOR deltas (Δ_t).
        Saves compacted patch to historical history and clears log buffer.
        """
        if not self.version_log:
            return {}

        compacted_deltas = {}
        initial_vals = {}

        # 1. Track initial state of modified indices prior to epoch edits
        for entry in self.version_log:
            idx = entry['index']
            if idx not in initial_vals:
                initial_vals[idx] = entry['old_val']

        # 2. Extract net changes (Initial ^ Final)
        for idx, start_val in initial_vals.items():
            final_val = self.db[idx]
            if start_val != final_val:
                compacted_deltas[idx] = start_val ^ final_val

        # 3. Store in multi-epoch history for versioned sync catch-up
        self.epoch_counter += 1
        self.historical_epoch_patches[self.epoch_counter] = compacted_deltas

        # 4. Reset version log buffer for the next epoch
        self.version_log.clear()
        return compacted_deltas

    def get_patches_since_epoch(self, client_epoch: int) -> dict:
        """
        Catches up a client that missed one or more epochs by combining all 
        historical deltas since client_epoch.
        """
        combined_deltas = {}
        for ep in range(client_epoch + 1, self.epoch_counter + 1):
            if ep in self.historical_epoch_patches:
                for idx, delta in self.historical_epoch_patches[ep].items():
                    combined_deltas[idx] = combined_deltas.get(idx, 0) ^ delta
        return combined_deltas
