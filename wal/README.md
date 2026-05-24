# WAL Subsystem

## File Structure

- `src/types.rs`: WAL entry schema and transaction/status enums.
- `src/log.rs`: append-only framed WAL writer/reader with checksum and tail repair.
- `src/pipeline.rs`: crash-safe write pipeline (streaming, chunk hash, WAL durable append before metadata CAS).
- `src/recovery.rs`: startup recovery and deterministic replay/abort handling.
- `src/error.rs`: WAL error model.
- `src/lib.rs`: exports and WAL integration tests.

## On-disk WAL Frame Format

Each append operation writes one frame:

1. `u32_le payload_len`
2. `payload` (JSON-encoded `WalEntry`)
3. `32-byte BLAKE3(payload)` checksum

Corrupt or truncated tails are detected at startup and truncated to last valid frame boundary.
