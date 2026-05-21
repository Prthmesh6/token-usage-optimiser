# Project Context: Enterprise AI Gateway (Go)

## 🎯 1. Core Purpose & Value Proposition
This system is a high-throughput, low-latency reverse proxy and semantic caching layer designed to sit between enterprise internal microservices and external LLM providers (e.g., OpenAI, Anthropic). 

It solves two primary enterprise infrastructure problems:
1. **Token-Aware Quota Protection:** Enforcing rate limits based on Tokens-Per-Minute (TPM) rather than traditional Requests-Per-Minute (RPM).
2. **Semantic Cost Reduction:** Intercepting semantically duplicate prompts (e.g., matching "How to reset password" with "Steps for password reset") using vector embeddings to bypass expensive external inference.

---

## 🏗️ 2. High-Level Architecture Flow
When an HTTP request hits the gateway:
1. **Ingress:** Listens on `:8080`, accepting standard OpenAI-compatible JSON payloads (`/v1/chat/completions`).
2. **Token Estimation:** Runs a fast, local tokenizer (`tiktoken`) to calculate the expected prompt tokens.
3. **Sliding Window Rate Limiter:** Evaluates the token balance against a Redis cluster using a tenant-isolated key. Rejects with `429` if exceeded.
4. **Context Isolation:** Separates system prompts/RAG context from the core user instruction. Hashes the system context using SHA-256.
5. **Semantic Cache Lookup:** Converts the user instruction into a vector using a fast embedding API call. Queries Redis Vector Search using a composite index: `Tenant_ID` + `Context_Hash` + `Vector_Similarity (Cosine > 0.95)`.
    * **Cache Hit:** Returns the stored response string immediately (~15ms, $0 cost).
    * **Cache Miss:** Forwards the request to the upstream LLM, streams the response to the user, and asynchronously writes the new response vector back to Redis with a configurable TTL.

---

## 💻 3. Production Tech Stack & Constraints
* **Language:** Go 1.22+ (Strictly prefer idiomatic, high-concurrency Go patterns using channels, sync pools, and context timeouts).
* **Router:** Standard `net/http` or lightweight framework (e.g., `chi` or `Fiber`). Avoid heavy magic.
* **Database & Vector Store:** Redis Stack (Redis JSON + Redis Search modules enabled).
* **Tokenization:** `github.com/pkoukk/tiktoken-go` (Local, zero network overhead).
* **Upstream Protocol:** HTTP client with connection pooling (`MaxIdleConns: 100`, `IdleConnTimeout: 90s`).

---

## 📂 4. Target Project Layout
Maintain a clean, standard Go project layout to align with production enterprise standards:

```text
enterprise-ai-gateway/
├── cmd/
│   └── gateway/
│       └── main.go           # Application entrypoint
├── internal/
│   ├── config/               # Environment variables and system flags
│   ├── handler/              # HTTP handlers (OpenAI contract matching)
│   ├── proxy/                # Reverse proxy routing logic to upstream LLMs
│   ├── limiter/              # Token-aware sliding window rate limiter
│   └── cache/                # Redis semantic vector cache implementation
├── pkg/
│   └── tokenizer/            # Wrapper around tiktoken utility functions
├── docker-compose.yml        # Setup for local Redis Stack development
├── gateway-context.md        # This context file (Source of Truth)
└── GOALS.md                  # Current roadmap milestones
```

---

## 🗄️ 5. Redis Data Schemas & Layouts

### Rate Limiter Key
* **Type:** Redis Sorted Set (ZSET)
* **Key Format:** `ratelimit:tenant:{tenant_id}:tpm`
* **Score:** Current Unix timestamp (milliseconds)
* **Value:** Unique request ID or random UUID string accompanied by token weight.

### Semantic Cache Index
* **Type:** Redis JSON document with an HNSW Vector Index applied.
* **Key Format:** `cache:tenant:{tenant_id}:ctx:{sha256_context_hash}:msg:{message_id}`
* **Schema Fields:**
    * `tenant_id` (string)
    * `context_hash` (string, SHA-256)
    * `prompt_raw` (string)
    * `response_raw` (string)
    * `prompt_vector` (VECTOR, Float32, Dimensions: 1536 for OpenAI embeddings, Metric: COSINE)

---

## 🛡️ 6. Rules for AI Coding Assistant (Bob)
1. **Never generate code using legacy packages** or deprecated Redis clients. Use `github.com/redis/go-redis/v9`.
2. **Every outbound network request** must honor the incoming `context.Context` for cancellation and timeouts.
3. **Handle partial JSON streaming** carefully if handling chunked LLM data.
4. **Do not hallucinate vector math** inside Go code; let Redis handle the vector distance computation via `FT.SEARCH`.
```

### Your Next Step with Bob
Create that file locally, and whenever you open a chat session with Bob or pass prompts to it, start by feeding it this file:

> *"Hey Bob, we are building a Go backend system. Here is our architectural specification and layout context: [Paste contents of gateway-context.md]. Keep this context in mind for all code generation. Let's start by writing the docker-compose.yml for our Redis Stack infrastructure and the main.go entrypoint boilerplate."*

How does this setup feel for kickstarting your local workflow?