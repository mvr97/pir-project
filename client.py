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
        for h_id in range(self.num_hints):
            chosen_blocks = sorted(random.sample(range(self.num_blocks), max(1, self.num_blocks // 2)))
            parity = 0
            indices = []
            for b in chosen_blocks:
                offset = self.iprf.eval(h_id, b)
                idx = b * self.w + offset
                indices.append(idx)
                parity ^= server_db[idx]
            
            self.hints.append({'blocks': chosen_blocks, 'parity': parity, 'indices': indices})

    def query(self, target_idx: int, server) -> tuple:
        """Executes an online PIR query for target_idx."""
        target_block = target_idx // self.w
        target_offset = target_idx % self.w

        candidate_hints = self.iprf.invert(target_block, target_offset)
        
        selected_hint_id = None
        for h_id in candidate_hints:
            if target_block in self.hints[h_id]['blocks']:
                selected_hint_id = h_id
                break

        if selected_hint_id is None:
            selected_hint_id = 0

        hint = self.hints[selected_hint_id]
        query_indices = [idx for idx in hint['indices'] if idx != target_idx]
        server_response = server.answer_query(query_indices)
        reconstructed_val = hint['parity'] ^ server_response
        return reconstructed_val, selected_hint_id

    def apply_epoch_patch(self, compacted_deltas: dict) -> int:
        """Applies compacted epoch updates to client hints in one batch."""
        updated_count = 0
        for idx, delta in compacted_deltas.items():
            b = idx // self.w
            off = idx % self.w
            affected_hints = self.iprf.invert(b, off)
            for h_id in affected_hints:
                if idx in self.hints[h_id]['indices']:
                    self.hints[h_id]['parity'] ^= delta
                    updated_count += 1
        return updated_count