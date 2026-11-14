import random

class PlinkoClient:
    """
    Client holding precomputed PIR hints.
    Uses iPRF inversion for fast hint searching and applying batched epoch updates.
    """
    def __init__(self, db_size: int, block_size: int, num_hints: int, iprf):
        self.N = db_size
        self.w = block_size
        self.num_blocks = db_size // block_size
        self.num_hints = num_hints
        self.iprf = iprf
        self.hints = []

    def offline_preprocessing(self, server_db: list):
        """Streams DB once offline to generate parity hints."""
        # A rebuild must replace the previous hint set, not append duplicates.
        self.hints.clear()
        for h_id in range(self.num_hints):
            chosen_blocks = sorted(random.sample(range(self.num_blocks), max(1, self.num_blocks // 2)))
            parity = 0
            indices = []
            for b in chosen_blocks:
                offset = self.iprf.eval(h_id, b)
                idx = b * self.w + offset
                indices.append(idx)
                parity ^= server_db[idx]
            
            self.hints.append({
                'blocks': chosen_blocks,
                'blocks_set': set(chosen_blocks),
                'parity': parity,
                'indices': indices
            })

    def query(self, target_idx: int, server) -> tuple:
        """Executes an online PIR query for target_idx."""
        target_block = target_idx // self.w
        target_offset = target_idx % self.w

        candidate_hints = self.iprf.invert(target_block, target_offset)
        
        selected_hint_id = None
        for h_id in candidate_hints:
            if target_block in self.hints[h_id]['blocks_set']:
                selected_hint_id = h_id
                break

        if selected_hint_id is None:
            raise LookupError(
                f"No hint covers index {target_idx} (block {target_block}, offset {target_offset}). "
                f"Increase num_hints (currently {self.num_hints}) or re-run preprocessing."
            )

        hint = self.hints[selected_hint_id]
        query_indices = [idx for idx in hint['indices'] if idx != target_idx]
        server_response = server.answer_query(query_indices)
        reconstructed_val = hint['parity'] ^ server_response
        return reconstructed_val, selected_hint_id

    def apply_epoch_patch(self, compacted_deltas: dict, server_db: list = None, rebuild_threshold: float = 1.0) -> int:
        """
        Applies compacted epoch updates to client hints in a single pass per hint.
        Includes Cost-Based Rebuild Threshold (default 1.0) & Per-Hint XOR Accumulation
        with O(1) block-membership verification and a single-item fast-path.
        """
        if not compacted_deltas:
            return 0

        # FAST-PATH: Single-entry patch (e.g., epoch_threshold == 1) updates parities directly
        if len(compacted_deltas) == 1:
            idx, delta = next(iter(compacted_deltas.items()))
            if delta == 0:
                return 0
            b = idx // self.w
            off = idx % self.w
            updated = 0
            for h_id in self.iprf.invert(b, off):
                if b in self.hints[h_id]['blocks_set']:
                    self.hints[h_id]['parity'] ^= delta
                    updated += 1
            return updated

        # 1. Cost-Based Rebuild Threshold Check
        if (len(compacted_deltas) / self.N) > rebuild_threshold and server_db is not None:
            print(f" [Threshold Exceeded] {len(compacted_deltas)} updates (>{rebuild_threshold * 100:.0f}% DB). Rebuilding hints offline...")
            self.offline_preprocessing(server_db)
            return len(self.hints)

        # 2. Per-Hint Delta Accumulation
        hint_deltas = {}

        for idx, delta in compacted_deltas.items():
            b = idx // self.w
            off = idx % self.w
            affected_hints = self.iprf.invert(b, off)
            
            for h_id in affected_hints:
                if b in self.hints[h_id]['blocks_set']:
                    hint_deltas[h_id] = hint_deltas.get(h_id, 0) ^ delta

        # 3. Single-Pass Update per Affected Hint
        for h_id, net_hint_delta in hint_deltas.items():
            self.hints[h_id]['parity'] ^= net_hint_delta

        return len(hint_deltas)
