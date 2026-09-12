# 🔌 Client SDK Integration Guides

VortexKV is **100% wire-compatible** with standard Redis clients. You do not need to install custom drivers — simply configure your existing Redis client to connect to port **`7379`**.

---

## 🐍 Python (`redis-py`)

Install official package:
```bash
pip install redis
```

Usage:
```python
import redis

# Connect to VortexKV wire port 7379
r = redis.Redis(
    host="localhost",
    port=7379,
    password="vortex_secure_2026",
    decode_responses=True
)

# Basic operations
r.set("user:1001", "Alice Vance", ex=300)
name = r.get("user:1001")
print(f"Retrieved: {name}")

# Hash structures
r.hset("session:xyz", mapping={"user_id": "1001", "ip": "192.168.1.1"})
session = r.hgetall("session:xyz")
print("Session:", session)

# Custom Vector Search command via raw execute_command
r.execute_command("VADD", "embeddings", "doc_1", 0.9, 0.1, 0.0)
top_matches = r.execute_command("VSEARCH", "embeddings", 1, "cosine", 0.88, 0.12, 0.0)
print("Vector Matches:", top_matches)
```

---

## 🟩 Node.js & TypeScript (`ioredis`)

Install package:
```bash
npm install ioredis
```

Usage:
```typescript
import Redis from "ioredis";

const vortex = new Redis({
  host: "127.0.0.1",
  port: 7379,
  password: "vortex_secure_2026",
});

async function run() {
  await vortex.set("config:theme", "cyberpunk");
  const theme = await vortex.get("config:theme");
  console.log("Current theme:", theme);

  // Atomic Transactions
  const pipeline = vortex.multi();
  pipeline.set("counter:step", "1");
  pipeline.incr("counter:step");
  const results = await pipeline.exec();
  console.log("Transaction executed:", results);

  // AI Vector Primitives
  await vortex.call("VADD", "vectors", "user_profile_1", "0.45", "0.85");
  const nearest = await vortex.call("VSEARCH", "vectors", "1", "cosine", "0.40", "0.80");
  console.log("Nearest vector:", nearest);

  vortex.disconnect();
}

run().catch(console.error);
```

---

## 🐹 Go (`go-redis/v9`)

Install package:
```bash
go get github.com/redis/go-redis/v9
```

Usage:
```go
package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func main() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:7379",
		Password: "vortex_secure_2026",
		DB:       0,
	})
	defer client.Close()

	// Write key with 10 minute expiration
	err := client.Set(ctx, "session:token", "alpha-omega-99", 10*time.Minute).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get(ctx, "session:token").Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read session: %s\n", val)

	// Call custom AI vector commands
	res, err := client.Do(ctx, "VSEARCH", "embeddings", 1, "cosine", 0.9, 0.1).Result()
	if err == nil {
		fmt.Printf("Vector search: %v\n", res)
	}
}
```

---

## ☕ Java / Kotlin (Jedis & Spring Data Redis)

### Maven Dependency
```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>5.1.0</version>
</dependency>
```

### Jedis Example
```java
import redis.clients.jedis.Jedis;

public class VortexDemo {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("localhost", 7379)) {
            jedis.auth("vortex_secure_2026");

            jedis.set("user:token", "jwt_secure_payload");
            String token = jedis.get("user:token");
            System.out.println("Token: " + token);

            // Lists and Queues
            jedis.rpush("task_queue", "job_1", "job_2");
            String nextJob = jedis.lpop("task_queue");
            System.out.println("Processing: " + nextJob);
        }
    }
}
```

---

## 🦀 Rust (`redis-rs`)

Add to `Cargo.toml`:
```toml
[dependencies]
redis = { version = "0.24", features = ["tokio-comp"] }
```

Usage:
```rust
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://:vortex_secure_2026@127.0.0.1:7379")?;
    let mut con = client.get_multiplexed_tokio_connection().await?;

    con.set("service:status", "active").await?;
    let val: String = con.get("service:status").await?;
    println!("Status: {}", val);

    Ok(())
}
```

---

## 🔷 C# / .NET (`StackExchange.Redis`)

Install via NuGet:
```bash
dotnet add package StackExchange.Redis
```

Usage:
```csharp
using StackExchange.Redis;

var connection = await ConnectionMultiplexer.ConnectAsync("localhost:7379,password=vortex_secure_2026");
IDatabase db = connection.GetDatabase();

await db.StringSetAsync("system:build", "v1.0.0-PROD");
string? version = await db.StringGetAsync("system:build");
Console.WriteLine($"VortexKV System Build: {version}");
```
