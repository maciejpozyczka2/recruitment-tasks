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

### Podejście do testowania

Kazdy test jest w pelni izolowany — helper `waitForMessage` / `collectMessages` tworzy nowego konsumenta z unikalnym groupId (UUID) i czyta od poczatku (`fromBeginning: true`), wiec testy mozna uruchomic w dowolnej kolejnosci bez wzajemnego zakaznienia.

Kluczowe mechanizmy:
- **Korelacja po unikalnym orderId** — zamiast zakladac pusty topic, kazdy test koreluje po unikalnym identyfikatorze (timestamp + random suffix), co eliminuje problemy z persistentnym Kafka (dane pomiedzy uruchomieniami).
- **Timeout z pollingiem** — zamiast sztywnych `setTimeout`/`sleep`, w `collectMessages` sprawdzam co 100ms, czy zebrano enough messages; w `waitForMessage` blokuję na timeout 20s domyslnie. To eliminuje czasowe flaky.
- **Promise waitera przed wyslaniem** — `waitForMessage` jest tworzone PRZED `sendOrder`/`sendRaw`, co eliminuje race condition (konsument moze zaczac czytac zanim wiadomosc zostanie wyslana).
- **Pozytywna kontrola w testach negatywnych** — przed sprawdzeniem, ze cos NIE trafi do DLQ, potwierdzam najpierw, ze zostalo przetworzone na `orders-processed` (test 6) lub ze komunikat jest na `orders-processed` (test 1, 5).
- **Asercje sprawdzaja nie tylko obecnosc, ale i poprawnosc** — typy pól (string/number), wartosci (status "PROCESSED"), unikalne identyfikatory, strukture bladów DLQ.

Pokryte sciezki:
1. **Poprawne przetwarzanie** — zamówienie trafia na `orders-processed` ze statusem "PROCESSED" (test 1, 5).
2. **Błędny JSON** — niepoprawny JSON trafia na `orders-dlq` z informacja o bledzie `Niepoprawny JSON` (test 2).
3. **Brakujace pola** — niekompletne zamówienie trafia na `orders-dlq` z informacja o `Brakujace pola` (test 3).
4. **Zly typ pola** — zly typ pola (amount jako string) trafia na `orders-dlq` z informacja o `musi byc liczba` (test 4).
5. **Wiele zamówien naraz** — 5 poprawnych zamówien trafia na `orders-processed` (test 5).
6. **Negatywna DLQ** — poprawne zamówienie NIE trafia na `orders-dlq` (test 6).
7. **Timeout helperów** — `waitForMessage` zwraca null gdy nie ma wiadomosci; `collectMessages` zwraca pusta tablice gdy nie ma enough messages (testy 7, 8).
8. **Tombstone** — pusta wiadomosc (tombstone) jest ignorowana bez logu i bez DLQ (test 9).
9. **Valid JSON with wrong shape (null)** — poprawny JSON o złym ksztalcie (null) blokuje partycje przez infinite retry kafkajs — brak DLQ i brak `orders-processed` (test 10).
10. **Valid JSON with wrong shape (string)** — poprawny JSON o złym ksztalcie (string) trafia na `orders-dlq` z informacja o `Brakujace pola` (test 11).
11. **orderId niebedacy stringiem** — orderId niebedacy stringiem trafia na `orders-dlq` z informacja o `musi byc stringiem` (test 12).
12. **Wiele bledow na raz** — wiele bledow na raz sklejone w jednym message error z DLQ (test 13).
13. **Propagacja klucza wiadomosci** — klucz wiadomosci jest propagowany na `orders-processed` (test 14).

### Znane ograniczenia / co zrobilbym/zrobilabym inaczej majac wiecej czasu

- **Brak testu negatywnego na innych topicach** — test 6 sprawdza, ze poprawne zamówienie NIE trafia na `orders-dlq`, ale nie weryfikuje, ze niekompletne zamówienie NIE trafia na `orders-processed`. Rozszerzenie o te asercje jest proste i wartego rozważenia.
- **Test 5 (wiele zamówien) nie weryfikuje kolejnosci** — w systemie Kafka kolejnosc w partycji jest gwarancyjna, ale test tego nie sprawdza. Warto dodać asercję `expect(receivedOrderIds).toEqual(expectedOrderIds)`.
- **Brak testów regresyjnych z losowymi timeoutami** — przy slabej wydajnoci Kafki timeout 20s moze byc za krótki w obciazonym srodowisku. Wartо rozważyć losowanie timeoutu w zakresie [10, 30] lub więcej.
- **Test niedzialania consumera** — brak testu symulujacego zatrzymanie kontenera Docker lub przerwe w przetwarzaniu. To wymagałoby modyfikacji kodu aplikacyjnego (np. mockowania `consumer.run()` aby zwróciło error) i nie jest wymagane w podstawie zadania, ale warto go rozważyć w przyszłości jako test resilience systemu.
- **Walidacja pól typu** — testy sprawdzaja obecność pól i typy (amount jako liczba, orderId jako string), ale nie pokrywaja wszystkich kombinacji (np. mixed types: amount jako string + orderId jako number).
- **Testy z różnymi rozmiarami partii** — pokryto partię 1 (test 1, 2, 3, 4, 6, 9, 10, 11, 12, 13) i 5 (test 5), ale nie 10/100. Wartо rozważyć testowanie z większymi partiami dla lepszego pokrycia wydajnosciowego.
- **Test key propagation na orders-dlq** — test 14 sprawdza propagacje klucza na `orders-processed`, ale nie sprawdza go na `orders-dlq`. To latwy dodatek, wartego rozważenia.
