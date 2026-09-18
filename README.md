## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

Na start wszystkie testy w `tests/__tests__/kafkaFlow.test.ts` są oznaczone
`it.skip(...)` — po zaimplementowaniu każdego testu zamień `it.skip` na `it`.

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia

- Nie wprowadzono żadnych zmian w konfiguracji Docker ani w kodzie aplikacji.
- Wymagana jest aktywna Docker Desktop z uruchomionym Docker Engine.
- W przypadku problemów z PowerShell (blad `npm.ps1 cannot be loaded`), uruchamiać komendy z `-ExecutionPolicy Bypass`.

### Podejście do testowania

Kazdy test jest w pelni izolowany - helper `waitForMessage` / `collectMessages` tworzy nowego konsumenta z unikalnym groupId i czyta od poczatku (fromBeginning: true), wiec testy mozna uruchomic w dowolnej kolejnosci. Zamiast sztywnych `setTimeout`/`sleep` uzywam polling z timeoutem (20s domyslnie), co eliminuje czasowe flaky. Asercje sprawdzaja nie tylko obecnosc wiadomosci, ale tez poprawnosc pól (typy, wartosci, status).

### Znane ograniczenia / co zrobilbym/zrobilabym inaczej majac wiecej czasu

- Brak testu negatywnego (valid order nie powinien tracic do DLQ) poza testem 6 - rozszerzenie o sprawdzenie braków na innych topicach.
- Test 5 (wiele zamowien) nie weryfikuje kolejnosci - w systemie Kafka kolejnosc w partitionie jest gwarancyjna, ale test tego nie sprawdza.
- Brak testów regresyjnych z losowymi timeoutami - przy slabej wydajnoci Kafki timeout 20s moze byc za krótki w obciazonym srodowisku.
- Wartо rozważyć testowanie z różnymi rozmiarami partii (1, 10, 100) dla lepszego pokrycia.
- Test niedzialania consumera (np. zatrzymanie kontenera Docker) - sprawdzenie, jak system radzi sobie z brakiem przetwarzania i backlogiem. Brak tego testu jest celowy, poniewaz wymaga modyfikacji kodu aplikacyjnego (symulacji przerwy w przetwarzaniu) i nie jest wymagany w podstawie zadania, ale warto go rozważyć w przyszłości jako test resilience systemu.
- Testy graniczne (null z waitForMessage, timeout w collectMessages) - dodane w celu sprawdzenia behavioru helperów w warunkach pressure (brak wiadomości, timeout).
