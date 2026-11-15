import pytest

from app.fingerprint import compute_fingerprint, normalize_message, top_frame
from app.rules import classify_by_rules, has_no_signal


@pytest.mark.parametrize(
    ("error_type", "message", "expected"),
    [
        ("AssertionError", "expected HTTP 402, got 200", "assertion"),
        ("org.junit.ComparisonFailure", "values differ", "assertion"),
        (None, "expected: <3> but was: <4>", "assertion"),
        ("TimeoutError", "DB connection timed out after 30s", "timeout"),
        ("RuntimeError", "operation timed out after 5000ms", "timeout"),
        ("ConnectionRefusedError", "[Errno 111]", "infrastructure"),
        ("RuntimeError", "could not reach db: Connection refused", "infrastructure"),
        ("IOError", "No space left on device", "infrastructure"),
        ("StaleElementReferenceException", "element is not attached", "flaky"),
        ("AssertionError", "intermittent: expected 1 got 2", "flaky"),
    ],
)
def test_rules_classify_known_patterns(error_type, message, expected):
    match = classify_by_rules(error_type, message, None)
    assert match is not None
    assert match.category == expected


def test_rules_leave_unrecognized_failures_to_llm():
    assert classify_by_rules("KeyError", "'customer_id'", None) is None
    assert classify_by_rules("NullPointerException", None, "at com.x.Foo.bar(Foo.java:10)") is None


def test_rules_only_look_at_stack_tail():
    deep_stack = "\n".join(
        ["File 'socket.py', line 1, in connect  # timeout=30"] + [f"frame {i}" for i in range(10)]
    )
    assert classify_by_rules("KeyError", "'x'", deep_stack) is None


def test_no_signal():
    assert has_no_signal(None, "", "  ")
    assert not has_no_signal("KeyError", None, None)


def test_normalize_message_strips_volatile_tokens():
    a = normalize_message("Order 12345 for user 'bob' failed at 10.0.0.7:5432 id=3f9a8b7c6d5e4f30")
    b = normalize_message("Order 99 for user 'alice' failed at 192.168.1.2:6543 id=0000aaaabbbbcccc")
    assert a == b


def test_fingerprint_groups_same_failure_with_different_details():
    stack1 = 'Traceback:\n  File "tests/test_x.py", line 12, in test_order\nKeyError: 12'
    stack2 = 'Traceback:\n  File "tests/test_x.py", line 40, in test_order\nKeyError: 99'
    f1 = compute_fingerprint("KeyError", "missing order 12", stack1)
    f2 = compute_fingerprint("KeyError", "missing order 99", stack2)
    assert f1.value == f2.value
    assert len(f1.value) == 32


def test_fingerprint_separates_different_failures():
    f1 = compute_fingerprint("KeyError", "missing order 12", None)
    f2 = compute_fingerprint("ValueError", "missing order 12", None)
    f3 = compute_fingerprint("KeyError", "missing customer 12", None)
    assert len({f1.value, f2.value, f3.value}) == 3


def test_top_frame_python_and_java():
    py = 'File "a.py", line 1, in outer\n  File "b.py", line 9, in inner'
    assert top_frame(py) == "b.py:inner"
    java = "java.lang.NullPointerException\n\tat com.acme.Cart.total(Cart.java:42)\n\tat com.acme.X.y(X.java:1)"
    assert top_frame(java) == "Cart.java:com.acme.Cart.total"
