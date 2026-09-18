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

    await sendOrder(producer, expectedOrder);

    const receivedOrder = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) =>
        value?.orderId === expectedOrder.orderId
    );

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
    const rawValue = "{ broken json";

    await sendRaw(producer, rawValue, "raw-key");

    const received = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      (value: Record<string, unknown>) => {
        return (
          !!value?.error &&
          typeof value.error === "string" &&
          value.error.includes("Niepoprawny JSON")
        );
      }
    );

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Niepoprawny JSON");
    expect(received).toHaveProperty("raw");
    expect(received.raw).toBe(rawValue);
  });

  it("Brakujace wymagane pola trafiaja na orders-dlq - w value pole error informuje o brakujacych polach", async () => {
    const incompleteOrder = {
      orderId: "missing-fields",
      customer: "Test Testowski",
    };

    await sendOrder(producer, incompleteOrder);

    const received = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      (value: Record<string, unknown>) => {
        return (
          !!value?.error &&
          typeof value.error === "string" &&
          value.error.includes("Brakujace pola")
        );
      }
    );

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("Brakujace pola");
    expect(received).toHaveProperty("originalMessage");
    expect(received.originalMessage).toHaveProperty("orderId", "missing-fields");
  });

  it("Zly typ pola amount (string zamiast liczby) trafia na orders-dlq - w value pole error informuje o zlym typie", async () => {
    const badTypeOrder = makeTestOrder({
      amount: "not-a-number" as unknown as number,
      orderId: `bad-type-${Date.now()}`,
    });

    await sendOrder(producer, badTypeOrder);

    const received = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      (value: Record<string, unknown>) => {
        return (
          !!value?.error &&
          typeof value.error === "string" &&
          value.error.includes("musi byc liczba")
        );
      }
    );

    expect(received).not.toBeNull();
    expect(received).toHaveProperty("error");
    expect(received.error).toContain("musi byc liczba");
  });

  it("Wiele poprawnych zamowien - wszystkie trafiaja na orders-processed", async () => {
    const orderCount = 5;
    const expectedOrderIds: string[] = [];

    for (let i = 0; i < orderCount; i++) {
      const order = makeTestOrder({
        orderId: `multi-order-${Date.now()}-${i}`,
      });
      expectedOrderIds.push(order.orderId);
      await sendOrder(producer, order);
    }

    const receivedOrders = await collectMessages(
      ORDERS_PROCESSED_TOPIC,
      (value: Record<string, unknown>) =>
        value?.status === "PROCESSED",
      orderCount
    );

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

    await sendOrder(producer, validOrder);

    const dlqMessage = await waitForMessage(
      ORDERS_DLQ_TOPIC,
      () => false,
      2
    );

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
});
