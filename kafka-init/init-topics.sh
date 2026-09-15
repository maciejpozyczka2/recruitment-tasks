#!/bin/bash
#
# Tworzy topici wymagane przez aplikację (orders, orders-processed, orders-dlq).
#
# Ten skrypt celowo NIE jest wklejony bezpośrednio do pola `command:` w
# docker-compose.yml, tylko montowany jako osobny plik. Docker Compose robi
# własną podmianę zmiennych (${VAR} / $VAR) w treści pliku YAML zanim command
# trafi do kontenera - przez to np. `$i` z pętli `for` byłoby błędnie
# potraktowane jako zmienna środowiskowa Compose, a nie zmienna powłoki
# wewnątrz kontenera. Osobny plik .sh eliminuje ten problem całkowicie.
set -ex

BOOTSTRAP_SERVER="kafka:9092"

# Dodatkowe zabezpieczenie: czekamy, aż broker faktycznie odpowiada na
# zapytania administracyjne, zanim spróbujemy utworzyć topici (na wypadek
# rzadkiego wyścigu czasowego tuż po tym, jak healthcheck zgłosi gotowość).
for i in $(seq 1 30); do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list > /dev/null 2>&1; then
    break
  fi
  echo "Broker jeszcze nie odpowiada, ponawiam ($i/30)..."
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create --if-not-exists --topic orders --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create --if-not-exists --topic orders-processed --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create --if-not-exists --topic orders-dlq --partitions 1 --replication-factor 1

echo "Topici utworzone poprawnie."
