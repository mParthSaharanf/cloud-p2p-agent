import time

import pytest

from agent.identity import AgentIdentity
from agent.messages import Vouch
from agent.trust import TrustEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_engine(max_depth: int = 3) -> TrustEngine:
    return TrustEngine(identity=AgentIdentity.generate(), max_depth=max_depth)


def chain_of(engines: list[TrustEngine], ttl: float = 3600.0) -> list[Vouch]:
    """Issue a contiguous vouch chain: engines[0] → engines[1] → ... → engines[-1]."""
    return [
        engines[i].issue_vouch(engines[i + 1].identity.agent_id, ttl_seconds=ttl)
        for i in range(len(engines) - 1)
    ]


# ---------------------------------------------------------------------------
# Trust anchor fast path
# ---------------------------------------------------------------------------

def test_trust_anchor_needs_no_chain():
    engine = make_engine()
    result = engine.verify_chain(engine.identity.agent_id, chain=[])
    assert result.trusted
    assert result.depth == 0


def test_adding_explicit_anchor():
    engine = make_engine()
    other = AgentIdentity.generate()
    engine.trust_anchors.add(other.agent_id)
    result = engine.verify_chain(other.agent_id, chain=[])
    assert result.trusted


# ---------------------------------------------------------------------------
# Valid chains
# ---------------------------------------------------------------------------

def test_single_hop_chain():
    root_engine = make_engine()
    leaf = AgentIdentity.generate()
    vouch = root_engine.issue_vouch(leaf.agent_id)

    verifier = make_engine()
    verifier.trust_anchors.add(root_engine.identity.agent_id)

    result = verifier.verify_chain(leaf.agent_id, [vouch])
    assert result.trusted
    assert result.depth == 1


def test_two_hop_chain():
    root_e = make_engine()
    mid_e = make_engine()
    leaf = AgentIdentity.generate()

    chain = chain_of([root_e, mid_e])
    chain.append(mid_e.issue_vouch(leaf.agent_id))

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)

    result = verifier.verify_chain(leaf.agent_id, chain)
    assert result.trusted
    assert result.depth == 2


def test_three_hop_chain_at_max_depth():
    engines = [make_engine() for _ in range(4)]  # root + 2 mid + leaf engine
    leaf = AgentIdentity.generate()

    chain = chain_of(engines)
    chain.append(engines[-1].issue_vouch(leaf.agent_id))

    verifier = make_engine(max_depth=3)
    verifier.trust_anchors.add(engines[0].identity.agent_id)

    # chain length is 3 (root→mid1, mid1→mid2, mid2→leaf)... wait,
    # engines has 4 entries so chain_of produces 3 vouches, then we append
    # one more making 4 — that exceeds max_depth=3, so test accordingly.
    # Correct: chain_of([e0,e1,e2,e3]) = 3 vouches; +1 = 4. Use 3 engines.
    engines = [make_engine() for _ in range(3)]
    leaf = AgentIdentity.generate()
    chain = chain_of(engines)               # 2 vouches
    chain.append(engines[-1].issue_vouch(leaf.agent_id))  # 3 vouches total

    verifier = make_engine(max_depth=3)
    verifier.trust_anchors.add(engines[0].identity.agent_id)
    result = verifier.verify_chain(leaf.agent_id, chain)
    assert result.trusted
    assert result.depth == 3


# ---------------------------------------------------------------------------
# Failure cases
# ---------------------------------------------------------------------------

def test_no_chain_for_unknown_subject():
    engine = make_engine()
    stranger = AgentIdentity.generate()
    result = engine.verify_chain(stranger.agent_id, chain=[])
    assert not result.trusted
    assert "no trust chain" in result.reason


def test_chain_root_not_a_trust_anchor():
    engine = make_engine()
    stranger_root = make_engine()
    leaf = AgentIdentity.generate()
    vouch = stranger_root.issue_vouch(leaf.agent_id)
    result = engine.verify_chain(leaf.agent_id, [vouch])
    assert not result.trusted
    assert "trust anchor" in result.reason


def test_chain_exceeds_max_depth():
    engines = [make_engine() for _ in range(5)]
    leaf = AgentIdentity.generate()
    chain = chain_of(engines)
    chain.append(engines[-1].issue_vouch(leaf.agent_id))  # 5 vouches > max_depth=3

    verifier = make_engine(max_depth=3)
    verifier.trust_anchors.add(engines[0].identity.agent_id)
    result = verifier.verify_chain(leaf.agent_id, chain)
    assert not result.trusted
    assert "max_depth" in result.reason


def test_expired_vouch_rejected():
    root_e = make_engine()
    leaf = AgentIdentity.generate()
    vouch = root_e.issue_vouch(leaf.agent_id, ttl_seconds=-1.0)  # already expired

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)
    result = verifier.verify_chain(leaf.agent_id, [vouch])
    assert not result.trusted
    assert "expired" in result.reason


def test_tampered_signature_rejected():
    root_e = make_engine()
    leaf = AgentIdentity.generate()
    vouch = root_e.issue_vouch(leaf.agent_id)

    # Tamper: swap subject to a different agent
    tampered = vouch.model_copy(update={"subject_id": AgentIdentity.generate().agent_id})

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)
    result = verifier.verify_chain(tampered.subject_id, [tampered])
    assert not result.trusted
    assert "signature" in result.reason


def test_chain_break_detected():
    root_e = make_engine()
    mid_e = make_engine()
    leaf = AgentIdentity.generate()

    vouch_root_to_mid = root_e.issue_vouch(mid_e.identity.agent_id)
    vouch_unrelated = make_engine().issue_vouch(leaf.agent_id)  # not mid_e issuing

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)
    result = verifier.verify_chain(leaf.agent_id, [vouch_root_to_mid, vouch_unrelated])
    assert not result.trusted
    # either "break" or "signature" depending on which check fires first
    assert not result.trusted


def test_chain_terminus_mismatch():
    root_e = make_engine()
    mid_e = make_engine()
    actual_subject = AgentIdentity.generate()
    claimed_subject = AgentIdentity.generate()

    vouch = root_e.issue_vouch(mid_e.identity.agent_id)
    vouch2 = mid_e.issue_vouch(actual_subject.agent_id)

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)
    # chain ends at actual_subject but we claim it's claimed_subject
    result = verifier.verify_chain(claimed_subject.agent_id, [vouch, vouch2])
    assert not result.trusted
    assert "terminus" in result.reason


# ---------------------------------------------------------------------------
# Revocation
# ---------------------------------------------------------------------------

def test_revoked_vouch_rejected():
    root_e = make_engine()
    leaf = AgentIdentity.generate()
    vouch = root_e.issue_vouch(leaf.agent_id)
    root_e.issue_revoke(leaf.agent_id)  # root revokes its own vouch

    verifier = make_engine()
    verifier.trust_anchors.add(root_e.identity.agent_id)
    # apply the revocation to the verifier too
    revoke_msg = root_e.issue_revoke(leaf.agent_id)
    verifier.apply_revoke(revoke_msg)

    result = verifier.verify_chain(leaf.agent_id, [vouch])
    assert not result.trusted
    assert "revoked" in result.reason


def test_apply_revoke_rejects_tampered_signature():
    root_e = make_engine()
    leaf = AgentIdentity.generate()
    revoke = root_e.issue_revoke(leaf.agent_id)
    tampered = revoke.model_copy(update={"signature": "badsig=="})

    engine = make_engine()
    applied = engine.apply_revoke(tampered)
    assert applied is False