import random

class PlinkoServer:
    """
    Server holding the raw database array.
    Implements a Versioned Update Log System and Patch Compaction
    as specified in Sections 3 and 5.2 of the Term Paper.
    """
    def __init__(self, db_size: int):
        self.N = db_size
        self.db = [random.randint(0, 255) for _ in range(self.N)]
        
        # VERSION LOG SYSTEM
        self.current_version = 0
        # Stores tuples: {'version': t, 'index': idx, 'old_val': A, 'new_val': B}
        self.version_log = []

    def answer_query(self, query_indices: list) -> int:
        """Computes XOR parity over requested indices without knowing target index."""
        parity = 0
        for idx in query_indices:
            parity ^= self.db[idx]
        return parity

    def receive_update(self, index: int, new_val: int):
        """
        Logs database changes into an explicit Version Log.
        Increments current database version t -> t+1.
        """
        old_val = self.db[index]
        self.current_version += 1
        
        # Record versioned change in log
        log_entry = {
            'version': self.current_version,
            'index': index,
            'old_val': old_val,
            'new_val': new_val
        }
        self.version_log.append(log_entry)
        
        # Apply update to server's raw database
        self.db[index] = new_val

    def end_epoch_and_compact(self) -> dict:
        """
        Processes the Version Log for the epoch.
        Compacts multi-step updates for identical indices into net XOR deltas (Δ_t).
        Clears log for the next epoch.
        """
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

        # 3. Reset version log buffer for the next epoch
        self.version_log.clear()
        return compacted_deltas