def test_quantum_circuit_decoherence():
    """Unhandled quantum hardware fault that avoids all assertion/timeout/infra rule keywords."""
    class QuantumPhaseFlipError(Exception):
        pass

    raise QuantumPhaseFlipError(
        "QPU qubit register state collapsed during unitary gate rotation. "
        "Fidelity dropped below threshold on superconducting transmon lattice. "
        "Uncalibrated microwave pulse envelope distorted phase coherence."
    )
