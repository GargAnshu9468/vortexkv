# 🔒 Security & Multi-User Access Control (ACL)

VortexKV provides enterprise production security supporting password authentication, Redis 6+ ACLs, and TLS wire encryption.

---

## 1. Password Authentication (`requirepass`)

Enforce client authentication:
```bash
./bin/vortex-server -requirepass "super_secret_password"
```

Clients must authenticate before issuing any command:
```bash
redis-cli -p 7379 -a "super_secret_password" PING
```

---

## 2. Multi-User Access Control Lists (ACL)

Manage fine-grained user permissions, key namespace restrictions, and roles:

- `ACL LIST`: Display all configured users and permission rules.
- `ACL SETUSER <username> on >password ~pattern* +@all`: Create or update a user.
- `ACL WHOAMI`: Return the current authenticated username.
- `AUTH <username> <password>`: Authenticate as a specific user.

Example namespace isolation:
```bash
# User can only access 'cache:*' keys:
ACL SETUSER cache_worker on >pass123 ~cache:* +@read +@write
```

---

## 3. Native TLS/SSL Wire Encryption

Encrypt traffic between clients and cluster nodes:
```bash
./bin/vortex-server -tls-cert cert.pem -tls-key key.pem
```
