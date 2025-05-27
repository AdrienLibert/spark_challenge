build:
	docker build -t local/spark-job -f src/Dockerfile .
	docker build -t local/spark-events -f spark-events/Dockerfile .

start:
	docker-compose up -d

stop:
	docker-compose down

clean:
	docker-compose rm -f