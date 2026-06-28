import time
import uuid

import pytest

from agent.messages import (
    MessageType,
    Vouch,
    Revoke,
    HandshakeRequest,
    HandshakeResponse,
    FileRequest,
    FileRequestAck,
    FileRequestReject,
    ChunkHeader,
    TransferComplete,
    ErrorMessage,
)


def test_vouch_round_trip():
    v = Vouch(
        issuer_id="issuer_pubkey_b64",
        subject_id="subject_pubkey_b64",
        expires_at=time.time() + 3600,
        signature="sig_b64",
    )
    restored = Vouch.model_validate_json(v.model_dump_json())
    assert restored == v
    assert restored.type == MessageType.VOUCH


def test_revoke_round_trip():
    r = Revoke(issuer_id="a", subject_id="b", signature="sig_b64")
    restored = Revoke.model_validate_json(r.model_dump_json())
    assert restored == r


def test_handshake_request_with_trust_chain():
    vouch = Vouch(
        issuer_id="root",
        subject_id="mid",
        expires_at=time.time() + 3600,
        signature="sig1",
    )
    req = HandshakeRequest(
        sender_id="mid",
        trust_chain=[vouch],
        nonce="random_nonce_b64",
    )
    restored = HandshakeRequest.model_validate_json(req.model_dump_json())
    assert restored.trust_chain[0] == vouch
    assert restored.nonce == "random_nonce_b64"


def test_handshake_response_round_trip():
    resp = HandshakeResponse(sender_id="agent_x", nonce_signature="sig_over_nonce")
    restored = HandshakeResponse.model_validate_json(resp.model_dump_json())
    assert restored == resp


def test_file_request_generates_unique_request_id():
    r1 = FileRequest(file_hash="abc123", range_start=0, range_end=1023, sender_id="x")
    r2 = FileRequest(file_hash="abc123", range_start=1024, range_end=2047, sender_id="x")
    assert r1.request_id != r2.request_id
    uuid.UUID(r1.request_id)  # raises if not a valid uuid4 string


def test_file_request_ack_reject_round_trip():
    ack = FileRequestAck(request_id="req-1", chunk_size=4096)
    assert FileRequestAck.model_validate_json(ack.model_dump_json()) == ack

    reject = FileRequestReject(request_id="req-1", reason="rate_limited", detail="quota hit")
    restored = FileRequestReject.model_validate_json(reject.model_dump_json())
    assert restored.reason == "rate_limited"


def test_file_request_reject_invalid_reason_rejected():
    with pytest.raises(Exception):
        FileRequestReject(request_id="req-1", reason="not_a_real_reason")


def test_chunk_header_round_trip():
    ch = ChunkHeader(
        request_id="req-1",
        range_start=0,
        range_end=4095,
        chunk_size=4096,
        checksum="deadbeef" * 8,
    )
    restored = ChunkHeader.model_validate_json(ch.model_dump_json())
    assert restored == ch


def test_transfer_complete_round_trip():
    tc = TransferComplete(request_id="req-1", total_bytes=1048576)
    assert TransferComplete.model_validate_json(tc.model_dump_json()) == tc


def test_error_message_optional_request_id():
    e = ErrorMessage(code="internal_error", detail="something broke")
    assert e.request_id is None
    restored = ErrorMessage.model_validate_json(e.model_dump_json())
    assert restored == e