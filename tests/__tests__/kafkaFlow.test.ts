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

    // Warmup: upewnij się że consumer aplikacji jest gotowy
    // Wyślij dummy order i czekaj aż się pojawi na orders-processed
    const warmupOrder = makeTestOrder({
      orderId: `warmup-${Date.now()}`,
    });

    const warmupPromise = waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) => value?.orderId === warmupOrder.orderId,
      10 // 10s timeout na warmup
    );

    await sendOrder(producer, warmupOrder);
    await warmupPromise;
  });

  afterAll(async () => {
    await disconnectProducer();
  });

  it("Przekazanie poprawnego zamowienia - status zmienia sie na PROCESSED i wiadomosc trafia na orders-processed", async () => {
    const expectedOrder = makeTestOrder({
      orderId: `valid-order-${Date.now()}`,
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
    const corrId = `broken-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const rawValue = `{ broken json ${corrId}`;

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id ||
      (typeof v?.raw === "string" && v.raw.includes(id));

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(corrId)
    );

    await sendRaw(producer, rawValue, "raw-key");

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Niepoprawny JSON");
    expect(received).toHaveProperty("raw");
    expect(received.raw).toBe(rawValue);
  });

  it("Brakujace wymagane pola trafiaja na orders-dlq - w value pole error informuje o brakujacych polach", async () => {
    const orderId = `missing-fields-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const incompleteOrder = {
      orderId,
      customer: "Test Testowski",
    };

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id;

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(orderId)
    );

    await sendOrder(producer, incompleteOrder);

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Brakujące pola");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", orderId);
  });

  it("Zly typ pola amount (string zamiast liczby) trafia na orders-dlq - w value pole error informuje o zlym typie", async () => {
    const orderId = `bad-type-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const badTypeOrder = makeTestOrder({
      amount: "not-a-number" as unknown as number,
      orderId,
    });

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id;

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(orderId)
    );

    await sendOrder(producer, badTypeOrder);

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("musi byc liczbą");
  });

  it("Wiele poprawnych zamowien - wszystkie trafiaja na orders-processed", async () => {
    const orderCount = 5;
    const testId = `multi-order-${Date.now()}`;
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
      orderId: `no-dlq-${Date.now()}`,
    });

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id;

    const dlqPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(validOrder.orderId),
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

  it("waitForMessage zwraca null gdy wiadomosc nie nadejdzie w czasie timeout", async () => {
    const result = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      () => false,
      2
    );

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

  it("Pusta wiadomosc (tombstone) jest ignorowana bez logu i bez DLQ", async () => {
    const result = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      () => false,
      5
    );

    expect(result).toBeNull();

    const dlqResult = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      () => false,
      5
    );

    expect(dlqResult).toBeNull();
  });

  it("Poprawny JSON o zlym ksztalcie (null) blokuje partycje - brak DLQ i brak orders-processed", async () => {
    await sendRaw(producer, "null", "test-null-key");

    const processedResult = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      () => false,
      5
    );

    expect(processedResult).toBeNull();

    const dlqResult = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      () => false,
      5
    );

    expect(dlqResult).toBeNull();
  });

  it("Poprawny JSON o zlym ksztalcie (string) trafia na orders-dlq z informacja o brakujacych polach", async () => {
    const corrId = `json-string-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const rawPayload = `"${corrId}"`;

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id ||
      (typeof v?.raw === "string" && v.raw.includes(id));

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(corrId),
      10
    );

    await sendRaw(producer, rawPayload, "test-string-key");

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Brakujące pola");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", corrId);
  });

  it("orderId niebedacy stringiem trafia na orders-dlq z informacja o zlym typie", async () => {
    const orderId = `bad-orderId-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const badOrder = makeTestOrder({
      orderId: 123 as unknown as string,
    });

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id;

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(orderId),
      10
    );

    await sendOrder(producer, badOrder);

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("musi byc stringiem");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", orderId);
  });

  it("Wiele bledow naraz sklejone jest w jednym message error z DLQ", async () => {
    const orderId = `multi-error-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const badOrder = makeTestOrder({
      amount: "not-a-number" as unknown as number,
      status: "",
      orderId,
    });

    const dlqMatcher = (id: string) => (v: any) =>
      v?.originalMessage?.orderId === id;

    const messagePromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      dlqMatcher(orderId),
      10
    );

    await sendOrder(producer, badOrder);

    const received = await messagePromise;

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("musi byc liczbą");
    expect(received.error).toContain("musi byc stringiem");
  });

  it("Klucz wiadomosci jest propagowany na orders-processed", async () => {
    const orderId = `key-test-${Date.now()}`;
    const order = makeTestOrder({
      orderId,
      customer: "Key Test Customer",
      amount: 42.0,
      status: "NEW",
    });

    await sendOrder(producer, order);

    const received = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (v: Record<string, unknown>) =>
        v?.orderId === orderId && v?.status === "PROCESSED"
    );

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("orderId", orderId);
    expect(received).toHaveProperty("status", "PROCESSED");
    expect(received.key).toBe(order.orderId);
  });
});
