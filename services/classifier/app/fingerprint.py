"""Failure fingerprinting: group failures that differ only in volatile details.

Two failures share a fingerprint when they have the same error type, the same
message after volatile tokens (numbers, hex ids, UUIDs, quoted values, paths'
line numbers) are replaced by placeholders, and the same top stack frame
(file and function, without the line number).
"""

import hashlib
import re
from dataclasses import dataclass

FINGERPRINT_VERSION = "1"
MAX_NORMALIZED_MESSAGE_CHARS = 500

_UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
_HEX = re.compile(r"\b(?:0x)?[0-9a-f]{8,}\b")
_IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
_QUOTED = re.compile(r"(['\"`])(?:(?!\1).){0,200}\1")
_NUMBER = re.compile(r"\b\d+(?:\.\d+)?(?:ms|s|m|h|kb|mb|gb)?\b")
_WHITESPACE = re.compile(r"\s+")
# Python: File "x.py", line 12, in func   |   Java: at com.x.Foo.bar(Foo.java:12)
_PY_FRAME = re.compile(r'File "([^"]+)", line \d+, in (\S+)')
_JAVA_FRAME = re.compile(r"\bat ([\w$.<>]+)\(([\w$.-]+):\d+\)")


@dataclass(frozen=True)
class Fingerprint:
    value: str
    normalized_message: str
    top_frame: str | None


def normalize_message(message: str | None) -> str:
    if not message:
        return ""
    text = message.strip().lower()
    text = _UUID.sub("<uuid>", text)
    text = _IPV4.sub("<ip>", text)
    text = _HEX.sub("<hex>", text)
    text = _QUOTED.sub("<str>", text)
    text = _NUMBER.sub("<n>", text)
    text = _WHITESPACE.sub(" ", text)
    return text[:MAX_NORMALIZED_MESSAGE_CHARS]


def top_frame(stack_trace: str | None) -> str | None:
    """Return the innermost stack frame (file:function) without line numbers."""
    if not stack_trace:
        return None
    py_frames = _PY_FRAME.findall(stack_trace)
    if py_frames:
        file_name, function = py_frames[-1]
        return f"{file_name}:{function}"
    java_frame = _JAVA_FRAME.search(stack_trace)
    if java_frame:
        return f"{java_frame.group(2)}:{java_frame.group(1)}"
    return None


def compute_fingerprint(
    error_type: str | None,
    error_message: str | None,
    stack_trace: str | None,
) -> Fingerprint:
    normalized = normalize_message(error_message)
    if not normalized and stack_trace:
        # Some reports put everything in the body; use its last line as the message.
        last_line = stack_trace.strip().splitlines()[-1] if stack_trace.strip() else ""
        normalized = normalize_message(last_line)
    frame = top_frame(stack_trace)
    material = "\x1f".join(
        [
            FINGERPRINT_VERSION,
            (error_type or "").strip().lower(),
            normalized,
            frame or "",
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return Fingerprint(value=digest, normalized_message=normalized, top_frame=frame)
