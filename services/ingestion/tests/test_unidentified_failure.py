def test_unidentified_system_crash():
    """Simulated unhandled kernel memory crash that rules do not know."""
    raise RuntimeError(
        "FATAL: Kernel trap 12: page fault while in kernel mode. "
        "Fault virtual address: 0xdeadbeef, code: 0x0002. "
        "Process heap allocator arena corruption in JIT runtime."
    )