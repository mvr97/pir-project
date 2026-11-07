import random

class SimpleIPRF:
    """
    Simulates an Invertible Pseudorandom Function (iPRF).
    Provides forward evaluation (hint_id, block_id -> offset)
    and O(1) inverse lookup (block_id, offset -> list of hint_ids).
    """
    def __init__(self, block_size, num_hints):
        self.block_size = block_size
        self.num_hints = num_hints
        self.forward_map = {}
        self.inverse_map = {}

    def eval(self, hint_id: int, block_id: int) -> int:
        """Forward evaluation: maps (hint_id, block_id) to a block offset."""
        key = (hint_id, block_id)
        if key not in self.forward_map:
            val = (hash((hint_id, block_id, 42)) % self.block_size)
            self.forward_map[key] = val
            
            if block_id not in self.inverse_map:
                self.inverse_map[block_id] = {off: [] for off in range(self.block_size)}
            self.inverse_map[block_id][val].append(hint_id)
            
        return self.forward_map[key]

    def invert(self, block_id: int, offset: int) -> list:
        """Inverse evaluation: Returns all hint_ids containing (block_id, offset) in O(1)."""
        return self.inverse_map.get(block_id, {}).get(offset, [])