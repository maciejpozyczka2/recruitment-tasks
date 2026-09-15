/**
 * TWOJE ZADANIE: zaimplementuj poniższe testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie,
 * a NIE pisanie aplikacji od nowa.
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 * Każdy test poniżej ma:
 *   - opis scenariusza w komentarzu,
 *   - `it.skip(...)` — zamień na `it(...)`, gdy zaimplementujesz test.
 *
 * Jeśli chcesz dodać własne testy wykraczające poza tę listę (np. dodatkowe
 * przypadki brzegowe) — jak najbardziej, będzie to dodatkowo punktowane.
 */
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  ORDERS_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();
  });

  afterAll(async () => {
    await disconnectProducer();
  });

  it.skip("przetwarza poprawną wiadomość (happy path)", async () => {
    // Scenariusz: poprawna wiadomość wysłana na topic 'orders' powinna
    // zostać przetworzona przez konsumenta i pojawić się na topicu
    // 'orders-processed' ze statusem "PROCESSED".
    //
    // Wskazówka: użyj makeTestOrder(), sendOrder() i waitForMessage().
  });

  it.skip("zachowuje oryginalne dane w przetworzonej wiadomości", async () => {
    // Scenariusz: dane z wiadomości wejściowej (orderId, customer, amount)
    // muszą zostać zachowane (niezmienione) w wiadomości wyjściowej —
    // zmienia się tylko pole 'status'.
  });

  it.skip("przetwarza wiele wiadomości bez utraty i duplikacji", async () => {
    // Scenariusz: wysłanie kilkunastu (np. 10) poprawnych wiadomości
    // powinno skutkować dokładnie taką samą liczbą przetworzonych
    // wiadomości na 'orders-processed' — bez utraty i bez duplikacji.
    //
    // Wskazówka: użyj collectMessages() z expectedCount.
  });

  it.skip("wysyła wiadomość z brakującym polem do DLQ", async () => {
    // Scenariusz: wiadomość, w której brakuje wymaganego pola
    // (np. 'amount'), NIE powinna trafić na 'orders-processed', tylko
    // na 'orders-dlq' wraz z opisem błędu.
    //
    // Wskazówka: zbuduj niepoprawne zamówienie na bazie makeTestOrder()
    // i usuń z niego jedno z wymaganych pól przed wysłaniem
    // (np. przez destrukturyzację albo `delete`).
  });

  it.skip("wysyła niepoprawny JSON do DLQ", async () => {
    // Scenariusz: wiadomość, która w ogóle nie jest poprawnym JSON-em
    // (np. zepsute bajty), powinna trafić na 'orders-dlq' z opisem błędu,
    // a nie wywalić konsumenta ani zniknąć bez śladu.
    //
    // Wskazówka: użyj sendRaw() z celowo niepoprawnym ciągiem znaków,
    // np. "{not-a-valid-json".
  });

  it.skip("przetwarza wiadomość w limicie czasu (SLA 10s)", async () => {
    // Scenariusz: poprawna wiadomość powinna zostać przetworzona
    // w rozsądnym czasie (przyjmij SLA testowe: 10 sekund od wysłania).
  });

  // --------------------------------------------------------------------
  // Miejsce na Twoje własne, dodatkowe testy (opcjonalnie).
  // Pomysły: kolejność wiadomości w obrębie jednej partycji, wiadomość
  // z niepoprawnym typem pola (np. amount jako string), zachowanie przy
  // bardzo dużej wiadomości, itp.
  // --------------------------------------------------------------------
});
