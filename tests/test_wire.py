"""The message framing."""

from helpers import expect_error, fixed_uuid

from schema_evolution.wire import HEADER_LENGTH, WireFormatError, frame, unframe


def test_frame_layout_matches_the_documented_format():
    version_id = fixed_uuid(7)
    message = frame(version_id, b"payload")
    assert message[0] == 3
    assert message[1] == 0
    assert message[2:18].hex() == version_id.replace("-", "")
    assert message[18:] == b"payload"
    assert len(message) == HEADER_LENGTH + len(b"payload")


def test_roundtrip_plain_and_compressed():
    version_id = fixed_uuid(9)
    for compress in (False, True):
        message = frame(version_id, b"abc" * 50, compress=compress)
        assert unframe(message) == (version_id, b"abc" * 50)
    assert frame(version_id, b"abc" * 50, compress=True)[1] == 5


def test_unframe_rejects_bad_messages():
    with expect_error(WireFormatError, "shorter"):
        unframe(b"\x03\x00")
    with expect_error(WireFormatError, "header version"):
        unframe(b"\xff" + b"\x00" * 30)
    with expect_error(WireFormatError, "compression"):
        unframe(bytes([3, 9]) + b"\x00" * 16 + b"x")
    with expect_error(WireFormatError, "zlib"):
        unframe(bytes([3, 5]) + b"\x00" * 16 + b"not zlib")


def test_frame_rejects_a_bad_schema_version_id():
    with expect_error(WireFormatError):
        frame("not-a-uuid", b"x")
