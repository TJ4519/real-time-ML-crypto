run-dev-live:
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	PRODUCT_IDS='["BTC/USD","ETH/EUR","ETH/USD"]' \
	LIVE_OR_HISTORICAL='live' \
	poetry run python main.py

run-dev-historical:
	# Same fix as above with proper variable setting and continuation
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	PRODUCT_IDS='["BTC/USD","ETH/EUR","ETH/USD"]' \
	LIVE_OR_HISTORICAL='historical' \
	poetry run python main.py

build:
	# Docker build
	docker build -t trade-producer .

run: build
	# Run docker container with proper network settings
	docker run \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e KAFKA_TOPIC_NAME=trade \
		-e PRODUCT_IDS='["BTC/USD"]' \
		-e LIVE_OR_HISTORICAL=live \
		trade-producer

run-historical: build
	# Create volume for historical run and execute with Docker
	docker volume create trade-producer-volume
	docker run \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e KAFKA_TOPIC_NAME=trade_historical \
		-e PRODUCT_IDS='["BTC/EUR", "ETH/USD"]' \
		-e LIVE_OR_HISTORICAL=historical \
		-e LAST_N_DAYS=2 \
		-e CACHE_DIR=/tmp/historical_trade_data \
		-v trade-producer-volume:/tmp/historical_trade_data \
		trade-producer

lint:
	# Lint the code using ruff
	poetry run ruff check --fix

format:
	# Format the code using ruff
	poetry run ruff format .

lint-and-format: lint format
