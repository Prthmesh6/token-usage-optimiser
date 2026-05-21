.PHONY: up down check-llama

up:
	@echo "🚀 Starting Redis Stack..."
	docker-compose up -d
	@echo "🚀 Starting Ollama daemon in background..."
	ollama serve > /dev/null 2>&1 & 
	@echo "⏳ Waiting for Ollama to boot..."
	sleep 3
	$(MAKE) check-llama
	@echo "✅ Infrastructure is UP! Redis GUI at http://localhost:8001"

down:
	@echo "🛑 Stopping Redis Stack..."
	docker-compose down
	@echo "🛑 Stopping Ollama daemon..."
	pkill -f "ollama serve" || true
	@echo "✅ Infrastructure is DOWN!"

check-llama:
	@echo "🧠 Ensuring Llama3 model is pulled..."
	ollama pull llama3