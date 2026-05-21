# 🛡️ Enterprise AI Gateway

A high-throughput, token-aware reverse proxy and semantic caching layer for Large Language Models (LLMs). Built to protect enterprise API quotas and reduce inference costs by intercepting duplicate semantic intents.

## 📖 The Problem

Standard API gateways rate-limit based on Requests-Per-Minute (RPM) and cache based on exact string matches. In the GenAI era, this fails:
1. LLM providers bill and rate-limit based on **Tokens-Per-Minute (TPM)**. A single massive request can exhaust a company's quota.
2. Exact-match caching misses variations in user intent (e.g., "Reset DB password" vs. "How do I reset my database password?"), resulting in duplicate inference costs.

## 🎯 The Solution

This gateway sits between internal enterprise microservices and external providers (OpenAI, Anthropic). It features:

* **Token-Aware Rate Limiting:** Intercepts payloads, estimates token burn using `tiktoken`, and enforces distributed TPM quotas via a Redis sliding window.
* **Semantic Caching:** Converts incoming prompts to dense vectors and queries a local vector database. If a semantically similar prompt (Cosine Similarity > 95%) was recently answered, it returns the cached response in milliseconds, bypassing the LLM entirely.
* **Strict Tenant Isolation:** Employs composite cache keys (Tenant ID + Context SHA-256 + Vector) to guarantee zero data leakage between different organizational contexts.

## 🏗️ Architecture Stack

* **Core Engine:** Go (Fiber or standard `net/http`)
* **State & Vector Store:** Redis (Redis Stack with Vector Search)
* **Inter-Service Communication:** gRPC (For scalable worker nodes)
* **Deployment:** Docker & Kubernetes (Manifests included for EKS)

## 🚀 Getting Started

### Prerequisites
* Go 1.22+
* Docker & Docker Compose
* An OpenAI API Key (for the fallback LLM and initial embedding tests)

### Local Spin-Up

1. **Clone the repository:**
   
```bash
   git clone [https://github.com/yourusername/enterprise-ai-gateway.git](https://github.com/yourusername/enterprise-ai-gateway.git)
   cd enterprise-ai-gateway
   ```

2. **Set your environment variables:**
   
```bash
   cp .env.example .env
   # Add your OPENAI_API_KEY to the .env file
   ```

3. **Boot the infrastructure (Redis Stack):**
   
```bash
   docker-compose up -d
   ```

4. **Run the Go Gateway:**
   
```bash
   go run main.go
   ```
   *The gateway will start listening on `localhost:8080`.*

## 🧪 Testing the Semantic Cache

Send a standard OpenAI ChatCompletion request to the gateway instead of OpenAI:

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer mock-tenant-token" \
  -d '{
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Explain what a distributed system is."}]
  }'
```

* **First Request:** Cache Miss. Takes ~3 seconds. Gateway queries OpenAI, returns response, and saves the vector embedding.
* **Second Request (Different Wording):** "What exactly is a distributed system?" -> Cache Hit. Takes ~15ms.

## 🛣️ Roadmap

- [x] Redis-backed sliding window for TPM limiting.
- [x] Basic Vector Semantic Caching.
- [ ] Add async Kafka logging for audit trails.
- [ ] Implement tiered TTLs (Time-To-Live) based on request classification.

## 🤝 Contributing
This project is built to demonstrate production-grade system design. Pull requests focusing on concurrency optimizations, benchmark tests, or memory profiling are highly encouraged.
```