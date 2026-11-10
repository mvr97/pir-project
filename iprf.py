class SimpleIPRF:
    """
    Simulates an Invertible Pseudorandom Function (iPRF).
    Provides forward evaluation (hint_id, block_id -> offset)
    and O(1) inverse lookup (block_id, offset -> list of hint_ids).
    """
    def __init__(self, block_size: int, num_hints: int):
        self.block_size = block_size
        self.num_hints = num_hints
        self.forward_map = {}
        self.inverse_map = {}

    def eval(self, hint_id: int, block_id: int) -> int:
        """
        Forward evaluation: maps (hint_id, block_id) to a deterministic block offset.
        Populates the reverse lookup table on first evaluation.
        """
        key = (hint_id, block_id)
        if key not in self.forward_map:
            # Deterministic pseudo-random offset computation
            val = hash((hint_id, block_id, 42)) % self.block_size
            self.forward_map[key] = val
            
            # Populate bi-directional inverse mapping
            if block_id not in self.inverse_map:
                self.inverse_map[block_id] = {off: [] for off in range(self.block_size)}
            
            # Avoid duplicate hint registrations
            if hint_id not in self.inverse_map[block_id][val]:
                self.inverse_map[block_id][val].append(hint_id)
            
        return self.forward_map[key]

    def invert(self, block_id: int, offset: int) -> list:
        """
        Inverse evaluation: Returns all hint_ids containing (block_id, offset) in O(1) time.
        """
        return self.inverse_map.get(block_id, {}).get(offset, [])

    def clear(self):
        """Resets mappings if hints are fully rebuilt."""
        self.forward_map.clear()
        self.inverse_map.clear()
