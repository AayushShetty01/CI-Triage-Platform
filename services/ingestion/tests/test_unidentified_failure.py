import pytest

def test_unhandled_corrupted_state_crash():
    """An obscure runtime crash that bypasses standard regex rules."""
    class KernelMemoryCorruption(Exception):
        pass

    raise KernelMemoryCorruption(
        "FATAL: Kernel signal 11 caught. Page table entry 0xdeadbeef unmapped. "
        "Native heap allocator arena chunk pointer corrupt during JIT execution."
    )
