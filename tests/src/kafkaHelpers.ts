/**
 * Helpery do testów Kafki.
 *
 * Ten plik jest już GOTOWY — nie musisz go modyfikować (choć oczywiście
 * możesz, jeśli uważasz, że coś warto poprawić lub rozszerzyć — to też
 * będzie pozytywnie ocenione, jeśli uzasadnisz zmianę w README).
 *
 * Do Twojej dyspozycji:
 *   - getProducer() / disconnectProducer() — połączony klient producenta.
 *   - makeTestOrder(overrides) — buduje przykładowe, poprawne zamówienie.
 *   - sendOrder(producer, order, topic?) — wysyła zamówienie (obiekt) na topic.
 *   - sendRaw(producer, rawValue, key?, topic?) — wysyła surowe dane
 *     (przydatne np. do testowania niepoprawnego JSON-a).
 *   - waitForMessage(topic, matchFn, timeoutSeconds?) — czeka aż na danym
 *     topicu pojawi się wiadomość spełniająca matchFn(value) -> boolean.
 *     Zwraca sparsowany obiekt albo null, jeśli nie znaleziono w limicie czasu.
 *   - collectMessages(topic, matchFn, expectedCount, timeoutSeconds?)
 *     — zbiera do expectedCount wiadomości spełniających matchFn.
 *
 * Każde wywołanie waitForMessage / collectMessages tworzy konsumenta
 * z UNIKALNYM groupId i czyta topic od początku (fromBeginning: true)
 * — dzięki temu testy są od siebie izolowane i nie interferują ze sobą,
 * niezależnie od kolejności uruchomienia.
 */

import { Kafka, Producer, Consumer, EachMessagePayload } from "kafkajs";
import { randomUUID } from "crypto";
 
export const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092";
export const ORDERS_TOPIC = "orders";
export const ORDERS_PROCESSED_TOPIC = "orders-processed";
export const ORDERS_DLQ_TOPIC = "orders-dlq";
 
const DEFAULT_POLL_TIMEOUT_SECONDS = 20;
 
const kafka = new Kafka({
  clientId: "kafka-qa-tests",
  brokers: [BOOTSTRAP_SERVERS],
  retry: {
    initialRetryTime: 2000,
    retries: 30,
  },
});
 
export interface Order {
  orderId: string;
  customer: string;
  amount: number;
  status: string;
}
 
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type MatchFn = (value: any) => boolean;
 
let sharedProducer: Producer | null = null;
 
/** Zwraca połączonego, współdzielonego producenta (tworzy go przy pierwszym wywołaniu). */
export async function getProducer(): Promise<Producer> {
  if (!sharedProducer) {
    sharedProducer = kafka.producer();
    await sharedProducer.connect();
  }
  return sharedProducer;
}
 
/** Rozłącza współdzielonego producenta — wywołaj w afterAll(). */
export async function disconnectProducer(): Promise<void> {
  if (sharedProducer) {
    await sharedProducer.disconnect();
    sharedProducer = null;
  }
}
 
/** Buduje przykładowe, poprawne zamówienie testowe z możliwością nadpisania pól. */
export function makeTestOrder(overrides: Partial<Order> = {}): Order {
  return {
    orderId: randomUUID(),
    customer: "Test Testowski",
    amount: 123.45,
    status: "NEW",
    ...overrides,
  };
}
 
/** Wysyła zamówienie (obiekt) jako JSON na wskazany topic (domyślnie 'orders'). */
export async function sendOrder(
  producer: Producer,
  order: Partial<Order>,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key: order.orderId, value: JSON.stringify(order) }],
  });
}
 
/**
 * Wysyła surowe (potencjalnie niepoprawne) dane, np. zepsuty JSON.
 * Przydatne do testowania obsługi błędnych wiadomości.
 */
export async function sendRaw(
  producer: Producer,
  rawValue: string | Buffer | null,
  key?: string,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key, value: rawValue }],
  });
}
 
function newConsumer(): Consumer {
  return kafka.consumer({ groupId: `test-${randomUUID()}` });
}
 
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function tryParse(value: Buffer | null): any | null {
  if (!value) return null;
  try {
    return JSON.parse(value.toString("utf-8"));
  } catch {
    return null;
  }
}
 
/**
 * Surowa wiadomość z topicu: klucz, sparsowana wartość (null dla tombstone
 * albo dla wartości, której nie da się sparsować) oraz oryginalny string.
 */
export interface RawMessage {
  key: string | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any | null;
  raw: string | null;
}
 
export type RawMatchFn = (message: RawMessage) => boolean;
 
/** Czeka aż warunek będzie spełniony albo minie timeout (sprawdza co 100 ms). */
function waitUntil(isSettled: () => boolean, timeoutSeconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    const deadline = Date.now() + timeoutSeconds * 1000;
    const interval = setInterval(() => {
      if (isSettled() || Date.now() >= deadline) {
        clearInterval(interval);
        resolve();
      }
    }, 100);
  });
}
 
/**
 * Wspólny silnik dla wszystkich helperów czytających: konsument z unikalnym
 * groupId czyta topic od początku i zbiera pasujące wiadomości, kończąc
 * od razu po zebraniu expectedCount (albo po upływie timeoutu).
 */
async function consumeMatching(
  topic: string,
  matchFn: RawMatchFn,
  expectedCount: number,
  timeoutSeconds: number
): Promise<RawMessage[]> {
  const consumer = newConsumer();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
 
  const found: RawMessage[] = [];
  let settled = false;
  let runnerError: Error | null = null;
 
  try {
    consumer
      .run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          if (settled) return;
          const candidate: RawMessage = {
            key: message.key ? message.key.toString("utf-8") : null,
            value: tryParse(message.value),
            raw: message.value ? message.value.toString("utf-8") : null,
          };
          if (matchFn(candidate)) {
            found.push(candidate);
            if (found.length >= expectedCount) {
              settled = true;
            }
          }
        },
      })
      .catch((err) => {
        runnerError = err;
      });
 
    await waitUntil(() => settled, timeoutSeconds);
 
    if (runnerError) throw runnerError;
  } finally {
    await consumer.disconnect();
  }
 
  return found;
}
 
/** Matcher na sparsowanej wartości — tombstone / niepoprawny JSON są pomijane. */
function byValue(matchFn: MatchFn): RawMatchFn {
  return (message) => message.value !== null && matchFn(message.value);
}
 
/**
 * Czeka aż w danym topicu pojawi się wiadomość spełniająca matchFn(value) -> boolean.
 * Zwraca sparsowaną wiadomość albo null, jeśli nie znaleziono w limicie czasu.
 */
export async function waitForMessage(
  topic: string,
  matchFn: MatchFn,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): Promise<any | null> {
  const [message] = await consumeMatching(topic, byValue(matchFn), 1, timeoutSeconds);
  return message ? message.value : null;
}
 
/** Zbiera do expectedCount wiadomości spełniających matchFn w limicie czasu. */
export async function collectMessages(
  topic: string,
  matchFn: MatchFn,
  expectedCount: number,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): Promise<any[]> {
  const messages = await consumeMatching(
    topic,
    byValue(matchFn),
    expectedCount,
    timeoutSeconds
  );
  return messages.map((message) => message.value);
}
 
/**
 * Wariant waitForMessage operujący na surowej wiadomości ({ key, value, raw }).
 * Potrzebny tam, gdzie testujemy sam klucz Kafki albo tombstone (value === null),
 * czego nie da się wyrazić matcherem na sparsowanej wartości.
 */
export async function waitForRawMessage(
  topic: string,
  matchFn: RawMatchFn,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<RawMessage | null> {
  const [message] = await consumeMatching(topic, matchFn, 1, timeoutSeconds);
  return message ?? null;
}
 
/** Wariant collectMessages operujący na surowych wiadomościach. */
export async function collectRawMessages(
  topic: string,
  matchFn: RawMatchFn,
  expectedCount: number,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<RawMessage[]> {
  return consumeMatching(topic, matchFn, expectedCount, timeoutSeconds);
}
 