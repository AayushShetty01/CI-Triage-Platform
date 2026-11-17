def test_unidentified_kernel_panic():
    """Unhandled fatal runtime crash that rules cannot categorize."""
    class HardwareMemoryFault(Exception):
        pass

    raise HardwareMemoryFault(
        "FATAL: Bus error (core dumped). Physical memory bus timeout at bank 3. "
        "ECC uncorrectable multibit fault detected during JIT page swap."
    )
