import pytest

from agent.messages import Vouch, FileRequest, ChunkHeader
from agent.serializer import encode, decode, decode_as, DeserializationError


def test_encode_decode_round_trip():
    v = Vouch(issuer_id="a", subject_id="b", expires_at=9999999999.0, signature="sig")
    wire = encode(v)
    assert isinstance(wire, bytes)
    restored = decode(wire)
    assert isinstance(restored, Vouch)
    assert restored == v


def test_decode_dispatches_to_correct_type():
    fr = FileRequest(file_hash="h", range_start=0, range_end=10, sender_id="x")
    restored = decode(encode(fr))
    assert isinstance(restored, FileRequest)


def test_decode_rejects_malformed_json():
    with pytest.raises(DeserializationError):
        decode(b"{not valid json")


def test_decode_rejects_missing_type_field():
    with pytest.raises(DeserializationError):
        decode(b'{"foo": "bar"}')


def test_decode_rejects_unknown_type():
    with pytest.raises(DeserializationError):
        decode(b'{"type": "not_a_real_type"}')


def test_decode_rejects_schema_mismatch():
    # valid type field, but missing required fields for that type
    with pytest.raises(DeserializationError):
        decode(b'{"type": "vouch", "issuer_id": "a"}')


def test_decode_as_enforces_expected_type():
    ch = ChunkHeader(
        request_id="r1", range_start=0, range_end=99, chunk_size=100, checksum="x" * 64
    )
    restored = decode_as(encode(ch), ChunkHeader)
    assert restored.request_id == "r1"


def test_decode_as_rejects_wrong_type():
    fr = FileRequest(file_hash="h", range_start=0, range_end=10, sender_id="x")
    with pytest.raises(DeserializationError):
        decode_as(encode(fr), ChunkHeader)