/**
 * TWOJE ZADANIE: zaimplementuj testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie,
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 */

/**
 * Testy E2E przepływu: producent -> `orders` -> konsument -> `orders-processed` / `orders-dlq`.
 *
 * Zasady przyjęte w tym pliku:
 *  - każdy test koreluje wiadomości po unikalnym identyfikatorze (orderId,
 *    kluczu Kafki albo polu `customer`), więc nie zakłada pustych topiców
 *    i jest odporny na dane z poprzednich uruchomień,
 *  - waiter (`waitForMessage` / `collectMessages`) jest tworzony PRZED wysyłką,
 *    żeby nie było wyścigu,
 *  - nic nie jest wysyłane na `orders` z niestringowym kluczem — `sendOrder`
 *    ustawia `key: order.orderId`, więc dla testów z niepoprawnym `orderId`
 *    używamy `sendRaw` z osobnym, poprawnym kluczem korelacyjnym.
 */

import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  collectMessages,
  collectRawMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
  waitForRawMessage,
} from "../src/kafkaHelpers";
 
/** Unikalny sufiks korelacyjny — chroni przed danymi z poprzednich uruchomień. */
function uniqueId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}
 
describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;
 
  beforeAll(async () => {
    producer = await getProducer();
 
    // Warmup: upewnij się że consumer aplikacji jest gotowy
    // Wyślij dummy order i czekaj aż się pojawi na orders-processed
    const warmupOrder = makeTestOrder({
      orderId: uniqueId("warmup"),
    });
 
    const warmupPromise = waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) => value?.orderId === warmupOrder.orderId,
      20
    );
 
    await sendOrder(producer, warmupOrder);
 
    if ((await warmupPromise) === null) {
      throw new Error(
        "Warmup nie doszedł na orders-processed — konsument aplikacji nie przetwarza wiadomości. " +
          "Sprawdź `docker compose logs consumer`; jeśli na topicu `orders` została zatruta " +
          "wiadomość (patrz ostatni, pominięty test w tym pliku), wyczyść środowisko: " +
          "`docker compose down -v && docker compose up --build -d`."
      );
    }
  });
 
  afterAll(async () => {
    await disconnectProducer();
  });
 
  it("Przekazanie poprawnego zamowienia - status zmienia sie na PROCESSED i wiadomosc trafia na orders-processed", async () => {
    const expectedOrder = makeTestOrder({
      orderId: uniqueId("valid-order"),
      customer: "Test Testowski",
      amount: 199.99,
      status: "NEW",
    });
 
    const messagePromise = waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) =>
        value?.orderId === expectedOrder.orderId
    );
 
    await sendOrder(producer, expectedOrder);
 
    const receivedOrder = await messagePromise;
 
    expect(receivedOrder).not.toBeNull();
    expect(receivedOrder).toHaveProperty("orderId", expectedOrder.orderId);
    expect(receivedOrder).toHaveProperty("customer", expectedOrder.customer);
    expect(receivedOrder).toHaveProperty("amount", expectedOrder.amount);
    expect(receivedOrder).toHaveProperty("status", "PROCESSED");
 
    expect(typeof receivedOrder.orderId).toBe("string");
    expect(typeof receivedOrder.customer).toBe("string");
    expect(typeof receivedOrder.amount).toBe("number");
    expect(Number.isFinite(receivedOrder.amount)).toBe(true);
  });
 
  it("Niepoprawny JSON trafia na orders-dlq - w value pole error zawiera informacje o niepoprawnym JSON", async () => {
    const corrId = uniqueId("broken");
    const rawValue = `{ broken json ${corrId}`;
 
    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => typeof v?.raw === "string" && v.raw.includes(corrId)
    );
 
    await sendRaw(producer, rawValue, corrId);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Niepoprawny JSON");
    expect(received).toHaveProperty("raw");
    expect(received.raw).toBe(rawValue);
  });
 
  it("Brakujace wymagane pola trafiaja na orders-dlq - w value pole error informuje o brakujacych polach", async () => {
    const orderId = uniqueId("missing-fields");
    const incompleteOrder = {
      orderId,
      customer: "Test Testowski",
    };
 
    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.orderId === orderId
    );
 
    await sendOrder(producer, incompleteOrder);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Brakujące pola: [amount, status]");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", orderId);
  });
 
  it("Niekompletne zamowienie NIE trafia na orders-processed", async () => {
    const orderId = uniqueId("missing-fields-not-processed");
 
    const processedPromise = waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (v: any) => v?.orderId === orderId,
      8
    );
 
    await sendOrder(producer, { orderId, customer: "Test Testowski" });
 
    // Potwierdzenie pozytywne: wiadomość faktycznie została przetworzona (trafiła do DLQ),
    // więc brak jej na orders-processed nie wynika z tego, że konsument jeszcze nie zdążył.
    const dlqMessage = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.orderId === orderId,
      15
    );
 
    expect(dlqMessage).not.toBeNull();
    expect(await processedPromise).toBeNull();
  });
 
  it("Zly typ pola amount (string zamiast liczby) trafia na orders-dlq - w value pole error informuje o zlym typie", async () => {
    const orderId = uniqueId("bad-type");
    const badTypeOrder = makeTestOrder({
      amount: "not-a-number" as unknown as number,
      orderId,
    });
 
    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.orderId === orderId
    );
 
    await sendOrder(producer, badTypeOrder);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Pole 'amount' musi być liczbą.");
    expect(received.originalMessage).toHaveProperty("amount", "not-a-number");
  });
 
  it("orderId niebedacy stringiem trafia na orders-dlq z informacja o zlym typie", async () => {
    // orderId jest liczbą, więc NIE można użyć sendOrder — ustawiłoby ono
    // key: 123, a kafkajs wymaga klucza string/Buffer (rzuciłoby TypeError
    // jeszcze przed wysłaniem). Korelujemy po unikalnym polu `customer`.
    const customer = uniqueId("bad-orderId-customer");
    const badOrder = { ...makeTestOrder(), orderId: 123, customer };
 
    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.customer === customer
    );
 
    await sendRaw(producer, JSON.stringify(badOrder), customer);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Pole 'orderId' musi być stringiem.");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", 123);
  });
 
  it("Wiele bledow naraz sklejone jest w jednym message error z DLQ", async () => {
    // Dwa niezależne błędy walidacji naraz: zły typ amount ORAZ zły typ orderId.
    // (samo amount + status: "" dawałoby tylko jeden błąd — status jest obecny,
    // a konsument nie waliduje jego wartości).
    const customer = uniqueId("multi-error-customer");
    const badOrder = {
      orderId: 456,
      customer,
      amount: "not-a-number",
      status: "NEW",
    };
 
    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.customer === customer
    );
 
    await sendRaw(producer, JSON.stringify(badOrder), customer);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Pole 'amount' musi być liczbą.");
    expect(received.error).toContain("Pole 'orderId' musi być stringiem.");
    // Błędy są sklejone separatorem "; " w jednym polu `error`.
    expect(received.error.split("; ")).toHaveLength(2);
  });
 
  it("Wiele poprawnych zamowien - wszystkie trafiaja na orders-processed", async () => {
    const orderCount = 5;
    const testId = uniqueId("multi-order");
    const expectedOrderIds: string[] = [];
 
    const collectPromise = collectMessages(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) => {
        const orderId = value?.orderId as string;
        return orderId?.startsWith(testId) && value?.status === "PROCESSED";
      },
      orderCount
    );
 
    for (let i = 0; i < orderCount; i++) {
      const order = makeTestOrder({
        orderId: `${testId}-${i}`,
      });
      expectedOrderIds.push(order.orderId);
      await sendOrder(producer, order);
    }
 
    const receivedOrders = await collectPromise;
 
    expect(receivedOrders).toHaveLength(orderCount);
 
    for (const received of receivedOrders) {
      expect(received).toHaveProperty("orderId");
      expect(received).toHaveProperty("customer");
      expect(received).toHaveProperty("amount");
      expect(received).toHaveProperty("status", "PROCESSED");
      expect(typeof received.orderId).toBe("string");
      expect(typeof received.customer).toBe("string");
      expect(typeof received.amount).toBe("number");
      expect(Number.isFinite(received.amount)).toBe(true);
    }
 
    const receivedOrderIds = receivedOrders.map((o) => o.orderId);
    for (const id of expectedOrderIds) {
      expect(receivedOrderIds).toContain(id);
    }
  });
 
  it("Poprawne zamowienie NIE trafia na orders-dlq - waitForMessage zwraca null", async () => {
    const validOrder = makeTestOrder({
      orderId: uniqueId("no-dlq"),
    });
 
    const dlqPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (v: any) => v?.originalMessage?.orderId === validOrder.orderId,
      8
    );
 
    await sendOrder(producer, validOrder);
 
    const processedMessage = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (v: Record<string, unknown>) =>
        v?.orderId === validOrder.orderId && v?.status === "PROCESSED",
      15
    );
 
    expect(processedMessage).not.toBeNull();
    expect(processedMessage).toHaveProperty("orderId", validOrder.orderId);
    expect(processedMessage).toHaveProperty("status", "PROCESSED");
 
    const dlqMessage = await dlqPromise;
 
    expect(dlqMessage).toBeNull();
  });
 
  it("Klucz wiadomosci jest propagowany na orders-processed", async () => {
    const orderId = uniqueId("key-test");
    const order = makeTestOrder({
      orderId,
      customer: "Key Test Customer",
      amount: 42.0,
      status: "NEW",
    });
 
    // waitForMessage zwraca samą sparsowaną wartość, a klucz Kafki nie jest
    // częścią payloadu — dlatego tu potrzebny jest wariant "raw".
    const messagePromise = waitForRawMessage(
      ORDERS_PROCESSED_TOPIC,
      (m) => m.value?.orderId === orderId && m.value?.status === "PROCESSED",
      15
    );
 
    await sendOrder(producer, order);
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received!.value).toHaveProperty("orderId", orderId);
    expect(received!.value).toHaveProperty("status", "PROCESSED");
    // sendOrder ustawia key === order.orderId, konsument przepisuje go dalej.
    expect(received!.key).toBe(order.orderId);
  });
 
  it("Klucz wiadomosci jest propagowany na orders-dlq", async () => {
    const orderId = uniqueId("key-dlq-test");
 
    const messagePromise = waitForRawMessage(
      ORDERS_DLQ_TOPIC,
      (m) => m.value?.originalMessage?.orderId === orderId,
      15
    );
 
    await sendOrder(producer, { orderId, customer: "Key DLQ Customer" });
 
    const received = await messagePromise;
 
    expect(received).not.toBeNull();
    expect(received!.key).toBe(orderId);
  });
 
  it("Pusta wiadomosc (tombstone) jest ignorowana - brak wpisu na orders-processed i orders-dlq", async () => {
    const key = uniqueId("tombstone");
    const followUp = makeTestOrder({ orderId: `${key}-follow-up` });
 
    // Tombstone (value === null), a zaraz po nim poprawne zamówienie z TYM SAMYM
    // kluczem — trafi na tę samą partycję, więc konsument przetworzy je już PO
    // tombstonie. Pojawienie się follow-upu dowodzi, że tombstone został
    // przeczytany i pominięty (a nie że konsument jeszcze nie zdążył).
    await sendRaw(producer, null, key);
    await sendRaw(producer, JSON.stringify(followUp), key);
 
    const followUpMessage = await waitForRawMessage(
      ORDERS_PROCESSED_TOPIC,
      (m) => m.value?.orderId === followUp.orderId,
      15
    );
 
    expect(followUpMessage).not.toBeNull();
 
    // Na obu topicach ma być dokładnie tyle wiadomości z tym kluczem, ile
    // wynika z follow-upu: jedna na orders-processed, zero na orders-dlq.
    const [processedWithKey, dlqWithKey] = await Promise.all([
      collectRawMessages(ORDERS_PROCESSED_TOPIC, (m) => m.key === key, 2, 5),
      collectRawMessages(ORDERS_DLQ_TOPIC, (m) => m.key === key, 1, 5),
    ]);
 
    expect(processedWithKey).toHaveLength(1);
    expect(processedWithKey[0].value).toHaveProperty("orderId", followUp.orderId);
    expect(dlqWithKey).toHaveLength(0);
  });
 
  it("waitForMessage zwraca null gdy wiadomosc nie nadejdzie w czasie timeout", async () => {
    const result = await waitForMessage(ORDERS_DLQ_TOPIC, () => false, 2);
 
    expect(result).toBeNull();
  });
 
  it("collectMessages zwraca pusta tablice gdy nie nadejdzie zadna wiadomosc w czasie timeout", async () => {
    const receivedOrders = await collectMessages(
      ORDERS_DLQ_TOPIC,
      () => false,
      10,
      3
    );
 
    expect(receivedOrders).toHaveLength(0);
  });
 
  it.skip("BUG: poprawny JSON nie bedacy obiektem (null) zatruwa partycje - brak DLQ i brak orders-processed", async () => {
    const key = uniqueId("poison-null");
 
    await sendRaw(producer, "null", key);
 
    const [processedResult, dlqResult] = await Promise.all([
      waitForRawMessage(ORDERS_PROCESSED_TOPIC, (m) => m.key === key, 10),
      waitForRawMessage(ORDERS_DLQ_TOPIC, (m) => m.key === key, 10),
    ]);
 
    // Oczekiwane zachowanie po naprawie: dlqResult !== null z informacją o błędzie.
    // Zachowanie obecne (udokumentowane tutaj): cisza na obu topicach.
    expect(processedResult).toBeNull();
    expect(dlqResult).toBeNull();
  });
});