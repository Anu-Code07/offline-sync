# Security Model

OrbitSync separates security policy from sync mechanics.

## Token handling

`SecureSyncConfig.tokenProvider` lets apps fetch short-lived tokens from
`flutter_secure_storage`, native keychain APIs, or an enterprise auth provider.

## Request signing

`SecureSyncConfig.requestSigner` can add HMAC, Ed25519, or tenant-specific
headers to every sync request.

## Encrypted storage

SQLite deployments can provide a SQLCipher-backed executor to
`SqliteStorageAdapter`. Hive and Isar deployments should provide encrypted boxes
or encrypted document bridges.

## Optional E2E encryption

`encrypt` and `decrypt` hooks are available for apps that need encrypted payloads
before records reach the transport layer. Conflict resolvers should run after
decryption so merges can preserve domain invariants.
