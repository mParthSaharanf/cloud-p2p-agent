---

## Test Suite

```bash
pytest tests/ -v
# 97 passed
```

---

## Production Considerations

**What the demo proves:**
- End-to-end file transfer between two cloud storage accounts
- Cryptographic peer identity and trust verification
- Concurrent multi-peer swarm splitting
- Zero local disk involvement

**What a production version would add:**
- `DriveAdapter` single-pass streaming upload (currently buffers chunks
  in memory; `google-resumable-media` library handles true streaming)
- Passphrase-encrypted private key storage
- Full vouch chain passing on every transfer request (currently bypassed
  with `P2P_TRUST_ALL=true` in dev)
- Redis-backed tracker registry for horizontal scaling
- Dropbox / S3 storage backends via the same `StorageBackend` interface
- Rate limiting and exponential backoff for Drive API quota (1000 req/100s)
- Agent-to-agent TLS for encrypted transport
- `pyproject.toml` for proper installable package

---

## Design Decisions Worth Discussing

**Why Ed25519 over RSA?**
Fixed 32-byte key size, faster signing, no padding oracle attacks, and
keys fit cleanly in JSON wire messages without base64-bloating the payload.

**Why manual dispatch over pydantic discriminated unions?**
These message types aren't nested under a parent envelope model — they're
top-level wire frames. Manual dispatch on the `type` field keeps the
serializer's error boundary clean: one `DeserializationError` type for all
failures (bad JSON, unknown type, schema mismatch) regardless of which
pydantic validation step failed.

**Why passive TTL expiry on tracker reads vs active sweep?**
An active background task adds complexity and a failure mode (task crashes,
stale peers accumulate). Passive expiry on read is simpler, correct, and
maps directly to Redis TTL behavior when the registry is productionized.

**Why separate bind host vs advertise host?**
Agents bind to `0.0.0.0` (all interfaces) but register their service
hostname (`agent_a`) with the tracker. Without this separation, peers
receive `0.0.0.0` as the connection address, which is meaningless from
another container.
