# Pre-Native LMDB Fixture

These databases were produced by a real Synor app run at revision
`63df53f605a552547cc016ef879d7cdf582e76e8`, before native revocation schema
version 3 existed. The generator uses only the public Python API and creates
root and child component state, memoization state, target tracking, and target
ownership for app `pre_native_fixture`.

LMDB database pages use the host page size, so the fixture includes 4 KiB and
16 KiB variants. Tests select the matching variant using `page_size::get()`.
`lock.mdb` is deliberately excluded because it is transient process state.

Fixture SHA-256:

```text
4096/data.mdb  fcfdae440098563ee91939e77b535971034969d554a8a477d543fb61d20554bb
16384/data.mdb 2153128f58e1b5ce2c667c8da86d963373cd8a2216c049c8d8ca281d21a25048
```

The 16 KiB fixture was generated on macOS arm64 with Python 3.11.15,
Rust 1.89.0, and Maturin 1.14.1. The 4 KiB fixture was generated with the same
source and tool versions in `rust:1.89-bookworm` image digest
`sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff`.

To reproduce, build/install revision `63df53f` in an isolated Python 3.11
environment and run:

```bash
python rust/core/tests/fixtures/pre_native_63df53f/generate.py /tmp/pre-native
```

Copy `/tmp/pre-native/mdb/data.mdb` into the directory matching the generator
host's page size, then update the recorded digest.
