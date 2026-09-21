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
 
- Nie wprowadzono żadnych zmian w konfiguracji Docker ani w kodzie aplikacji
  (`consumer/`, `producer/`).
- Rozszerzono `tests/src/kafkaHelpers.ts` — uzasadnienie niżej.
- Cały zestaw przechodzi na czystym środowisku (`docker compose down -v && docker compose up --build -d`)
  oraz w kolejnych przebiegach bez resetu (topici z danymi z poprzednich uruchomień) — ~96 s.
 
### Zmiany w helperach i ich uzasadnienie
 
1. **`waitForRawMessage` / `collectRawMessages`** — matcher dostaje surową wiadomość
   `{ key, value, raw }` zamiast samej sparsowanej wartości. Bez tego nie da się
   napisać dwóch testów: propagacji klucza Kafki (klucz nie jest częścią payloadu,
   więc `received.key` na sparsowanej wartości zawsze jest `undefined`) oraz
   tombstone'a (`value === null`, a stary matcher pomijał takie wiadomości).
2. **Wcześniejsze kończenie oczekiwania** — `waitForMessage` czekało zawsze pełny
   timeout (`setTimeout` na `timeoutSeconds`), nawet po znalezieniu wiadomości.
   Teraz oba helpery używają tego samego pollingu co pierwotny `collectMessages`
   i kończą od razu po zebraniu oczekiwanej liczby wiadomości. Czas zestawu
   spadł ze 181 s do 96 s, bez zmiany semantyki (testy negatywne nadal czekają
   pełny timeout, bo nic nie nadchodzi).
3. **Wspólny silnik `consumeMatching`** — cztery publiczne helpery to dziś cienkie
   nakładki na jedną funkcję zamiast czterech kopii logiki connect/run/disconnect.
4. **`sendRaw` przyjmuje `null`** — potrzebne do wysłania prawdziwego tombstone'a.
 
API publiczne (`waitForMessage`, `collectMessages`, `sendOrder`, `sendRaw`, …)
pozostało zgodne wstecz.
 
### Podejście do testowania
 
Każdy test jest w pełni izolowany — helpery tworzą konsumenta z unikalnym
groupId (UUID) i czytają topic od początku (`fromBeginning: true`), więc testy
można uruchamiać w dowolnej kolejności.
 
Kluczowe mechanizmy:
- **Korelacja po unikalnym identyfikatorze** — `uniqueId()` (timestamp + losowy
  sufiks) w `orderId`, kluczu Kafki albo w polu `customer`. Żaden test nie zakłada
  pustego topicu, więc persystentne dane Kafki nie psują kolejnych przebiegów.
- **Waiter przed wysyłką** — `waitForMessage` jest tworzone PRZED `sendOrder`/`sendRaw`,
  co eliminuje wyścig.
- **Kontrola pozytywna w testach negatywnych** — zanim stwierdzę „wiadomości NIE ma
  na topicu X", potwierdzam, że konsument ją faktycznie przetworzył (trafiła na
  drugi topic). Inaczej test przechodziłby również przy martwym konsumencie.
  Dotyczy testów: niekompletne zamówienie nie na `orders-processed`, poprawne
  zamówienie nie na `orders-dlq`, tombstone.
- **Tombstone testowany przez follow-up** — po tombstonie wysyłam poprawne zamówienie
  z tym samym kluczem (ta sama partycja → gwarantowana kolejność). Pojawienie się
  follow-upu dowodzi, że tombstone został przeczytany i pominięty; potem sprawdzam,
  że z tym kluczem jest dokładnie jedna wiadomość na `orders-processed` i zero na DLQ.
- **`sendRaw` zamiast `sendOrder` dla niestringowego `orderId`** — `sendOrder` ustawia
  `key: order.orderId`, a kafkajs wymaga klucza `string`/`Buffer`; przy `orderId: 123`
  rzuca `TypeError` jeszcze przed wysłaniem i test nigdy nie dotyka aplikacji.
  Dlatego takie przypadki idą przez `sendRaw` z osobnym, poprawnym kluczem korelacyjnym.
- **Asercje na dokładnych komunikatach** — np. `Brakujące pola: [amount, status]`,
  `Pole 'amount' musi być liczbą.` (z polskimi znakami, dokładnie jak w `validate()`),
  oraz sprawdzenie, że wiele błędów jest sklejonych separatorem `"; "` w jednym polu `error`.
 
Pokryte ścieżki:
1. Poprawne zamówienie → `orders-processed` ze statusem `PROCESSED` (pojedyncze i 5 naraz).
2. Niepoprawny JSON → `orders-dlq` z `Niepoprawny JSON` i oryginalnym `raw`.
3. Brakujące pola → `orders-dlq` z listą brakujących pól + brak wpisu na `orders-processed`.
4. Zły typ `amount` → `orders-dlq`.
5. Zły typ `orderId` → `orders-dlq`.
6. Wiele błędów naraz sklejonych w jednym polu `error`.
7. Poprawne zamówienie NIE trafia na `orders-dlq`.
8. Propagacja klucza Kafki na `orders-processed` i na `orders-dlq`.
9. Tombstone jest ignorowany (bez DLQ i bez `orders-processed`).
10. Zachowanie helperów przy timeoucie (`null` / pusta tablica).
 
### Znalezione błędy aplikacji
 
**Poison pill: poprawny JSON, który nie jest obiektem, zabija konsumenta.**
Dla wartości `null`, `"tekst"` czy `123` `JSON.parse` się udaje, po czym
`validate()` wykonuje `field in order` na wartości nie-obiektowej i rzuca
`TypeError`. Konsument tego nie łapie: wiadomość nie trafia ani na
`orders-processed`, ani na `orders-dlq`, kafkajs ponawia batch, a po wyczerpaniu
retry konsument umiera — grupa `orders-processing-service` zostaje bez aktywnych
członków, choć kontener nadal jest `Up` (błąd runnera nie dociera do `main().catch`,
bo `consumer.run()` zwraca natychmiast). Offset nigdy nie zostaje scommitowany,
więc partycja jest zablokowana także dla kolejnych uruchomień testów —
jedynym wyjściem jest `docker compose down -v`.
 
Test dokumentujący to zachowanie jest w pliku jako **`it.skip`** (ostatni),
bo jego uruchomienie trwale psuje środowisko dla pozostałych testów.
Proponowana poprawka po stronie aplikacji: odrzucać na DLQ wszystko, co nie jest
obiektem (`typeof order !== "object" || order === null || Array.isArray(order)`),
i dodatkowo opakować całe `eachMessage` w `try/catch` z wysyłką na DLQ, żeby
żaden nieprzewidziany wyjątek nie blokował partycji.
 
### Znane ograniczenia / co zrobiłbym mając więcej czasu
 
- **Czas trwania zestawu (~96 s)** — dominuje go koszt dołączania do grupy przez
  konsumenta tworzonego per wywołanie helpera (~5 s na test). Jeden współdzielony
  konsument na topic z buforem wiadomości i subskrypcjami per test skróciłby to
  do kilkunastu sekund, kosztem prostoty i izolacji.
- **Brak testu kolejności w partycji** — test „wiele zamówień" sprawdza komplet
  identyfikatorów, ale nie porządek; przy stałym kluczu dałoby się asertować
  `toEqual` zamiast `toContain`.
- **Brak testów odpornościowych** — restart konsumenta w trakcie przetwarzania,
  rebalans, duplikaty przy at-least-once delivery.
- **Brak testu na `amount` ujemny / `status` spoza słownika** — aplikacja ich nie
  waliduje, więc byłyby to testy oczekiwanego braku walidacji; warto ustalić
  z produktem, czy to świadoma decyzja.