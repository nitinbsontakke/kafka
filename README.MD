
<!-- Clean broker state before starting -->
docker-compose down -v   # removes containers and volumes
rm -rf /tmp/kraft-combined-logs  # clear local KRaft broker logs if you are mapping volumes
docker-compose up
<!-- This ensures MM2 starts only after brokers are ready. -->
command: ["sh", "-c", "until nc -z primary-kafka 9092; do sleep 1; done; /kafka/config/mm2.properties"]


# Stop and remove all containers from docker-compose
docker-compose down

# Remove all dangling containers
docker container prune -f

# Then rebuild and restart
docker build -f docker/kafka/Dockerfile/dockerfile -t kafka-mirror-maker:latest .
docker-compose up 


#for producer
docker build -f Producer/Dockerfile/dockerfile -t producer:latest .


export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH