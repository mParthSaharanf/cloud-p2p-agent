import base64

from agent.identity import AgentIdentity


def test_generate_produces_valid_agent_id():
    identity = AgentIdentity.generate()
    raw = base64.b64decode(identity.agent_id)
    assert len(raw) == 32  # Ed25519 public keys are always 32 bytes


def test_sign_and_verify_round_trip():
    identity = AgentIdentity.generate()
    data = b"hello agent network"
    sig = identity.sign(data)
    assert AgentIdentity.verify(identity.agent_id, data, sig) is True


def test_verify_fails_on_tampered_data():
    identity = AgentIdentity.generate()
    sig = identity.sign(b"original data")
    assert AgentIdentity.verify(identity.agent_id, b"tampered data", sig) is False


def test_verify_fails_with_wrong_public_key():
    identity_a = AgentIdentity.generate()
    identity_b = AgentIdentity.generate()
    sig = identity_a.sign(b"some message")
    assert AgentIdentity.verify(identity_b.agent_id, b"some message", sig) is False


def test_verify_fails_on_malformed_agent_id():
    identity = AgentIdentity.generate()
    sig = identity.sign(b"data")
    assert AgentIdentity.verify("not-valid-base64!!!", b"data", sig) is False


def test_verify_fails_on_malformed_signature():
    identity = AgentIdentity.generate()
    assert AgentIdentity.verify(identity.agent_id, b"data", "not-valid-base64!!!") is False


def test_each_identity_has_unique_agent_id():
    a = AgentIdentity.generate()
    b = AgentIdentity.generate()
    assert a.agent_id != b.agent_id


def test_load_or_create_generates_when_missing(tmp_path):
    key_path = tmp_path / "identity.pem"
    assert not key_path.exists()
    identity = AgentIdentity.load_or_create(key_path)
    assert key_path.exists()


def test_load_or_create_persists_same_identity_across_calls(tmp_path):
    key_path = tmp_path / "identity.pem"
    first = AgentIdentity.load_or_create(key_path)
    second = AgentIdentity.load_or_create(key_path)
    assert first.agent_id == second.agent_id


def test_load_or_create_sets_restrictive_permissions(tmp_path):
    key_path = tmp_path / "identity.pem"
    AgentIdentity.load_or_create(key_path)
    mode = key_path.stat().st_mode & 0o777
    assert mode == 0o600


def test_loaded_identity_can_sign_and_verify(tmp_path):
    key_path = tmp_path / "identity.pem"
    first = AgentIdentity.load_or_create(key_path)
    sig = first.sign(b"persisted key test")
    second = AgentIdentity.load_or_create(key_path)
    assert AgentIdentity.verify(second.agent_id, b"persisted key test", sig) is True