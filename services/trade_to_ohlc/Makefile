build:
	docker build -t trade-ohlc-converter .
run:
	docker run --network=redpanda_network trade-ohlc-converter


#New set of 
run-dev:
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	source setup_live_config.sh && poetry run python src/main.py

run-dev-historical:
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	source setup_historical_config.sh && poetry run python src/main.py

build:
	docker build -t trade-to-ohlc .

run: build
	docker run \
		--network=redpanda_network \
		--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		--env KAFKA_INPUT_TOPIC_NAME=trade \
		--env KAFKA_OUTPUT_TOPIC_NAME=ohlc \
		--env KAFKA_CONSUMER_GROUP=trade_to_ohlc_consumer_group \
		--env OHLC_WINDOW_SECONDS=60 \
		trade-to-ohlc

run-historical: build
	docker run \
		--network=redpanda_network \
		--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		--env KAFKA_INPUT_TOPIC_NAME=trade_historical \
		--env KAFKA_OUTPUT_TOPIC_NAME=ohlc_historical \
		--env KAFKA_CONSUMER_GROUP=trade_to_ohlc_historical_consumer_group \
		--env OHLC_WINDOW_SECONDS=60 \
		trade-to-ohlc

lint:
	poetry run ruff check --fix

format:
	poetry run ruff format .

lint-and-format: lint format