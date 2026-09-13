# 🧠 Native AI Vector Search (HNSW)

VortexKV allows storing high-dimensional vector embeddings and performing top-K nearest neighbor searches natively over standard Redis protocol without external plugins.

---

## ⚡ Vector Commands Reference

### 1. `VADD <index> <id> <dim1> <dim2> ... <dimN>`
Stores a high-dimensional vector in index `<index>` associated with `<id>`.

```bash
# Add document embeddings (4-dimensional example):
redis-cli -p 7379 -a "vortex_secure_2026" VADD articles doc_ai 0.95 0.05 0.0 0.0
redis-cli -p 7379 -a "vortex_secure_2026" VADD articles doc_quantum 0.1 0.9 0.0 0.0
redis-cli -p 7379 -a "vortex_secure_2026" VADD articles doc_crypto 0.2 0.3 0.9 0.0
```

### 2. `VSEARCH <index> <topK> <metric> <q1> <q2> ... <qN>`
Searches the top-K nearest neighbors to query vector `[q1, ..., qN]`.
Supported metrics: `cosine`, `euclidean`, `dot`.

```bash
redis-cli -p 7379 -a "vortex_secure_2026" VSEARCH articles 2 cosine 0.90 0.10 0.0 0.0
# 1) 1) "doc_ai"
#    2) "0.999512"
# 2) 1) "doc_crypto"
#    2) "0.452110"
```

### 3. `VSIM <index> <id1> <id2> <metric>`
Calculates similarity score directly between two stored vectors.

```bash
redis-cli -p 7379 -a "vortex_secure_2026" VSIM articles doc_ai doc_quantum cosine
# "0.141421"
```

### 4. `VDEL <index> <id>`
Deletes a vector from the index.

```bash
redis-cli -p 7379 -a "vortex_secure_2026" VDEL articles doc_crypto
# (integer) 1
```

### 5. `VINFO <index>`
Retrieves stats and dimensions of a vector index.

```bash
redis-cli -p 7379 -a "vortex_secure_2026" VINFO articles
# 1) "index_name"
# 2) "articles"
# 3) "dimensions"
# 4) (integer) 4
# 5) "total_vectors"
# 6) (integer) 2
```
