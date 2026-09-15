/**
 * Prosty producent Kafka.
 *
 * Wysyła losowe/przykładowe zamówienia na topic `orders`.
 * Służy jako demonstracyjny klient aplikacji - w rzeczywistym scenariuszu
 * to mogłaby być np. usługa e-commerce publikująca zdarzenia o zamówieniach.
 */
import { Kafka, Producer, logLevel } from "kafkajs";
import { randomUUID } from "crypto";

const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092";
const TOPIC = process.env.ORDERS_TOPIC || "orders";
const ORDER_COUNT = parseInt(process.env.ORDER_COUNT || "10", 10);
const INTERVAL_SECONDS = parseFloat(process.env.INTERVAL_SECONDS || "1");

const CUSTOMERS = ["Jan Kowalski", "Anna Nowak", "Piotr Wiśniewski", "Maria Wójcik"];

interface Order {
  orderId: string;
  customer: string;
  amount: number;
  status: string;
}

function log(message: string): void {
  console.log(`${new Date().toISOString()} [producer] ${message}`);
}

function buildOrder(): Order {
  const customer = CUSTOMERS[Math.floor(Math.random() * CUSTOMERS.length)];
  const amount = Math.round((Math.random() * (999 - 10) + 10) * 100) / 100;
  return {
    orderId: randomUUID(),
    customer,
    amount,
    status: "NEW",
  };
}

function sleep(seconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}

async function main(): Promise<void> {
  const kafka = new Kafka({
    clientId: "orders-producer",
    brokers: [BOOTSTRAP_SERVERS],
    logLevel: logLevel.NOTHING,
    retry: {
      initialRetryTime: 2000,
      retries: 30,
    },
  });

  const producer: Producer = kafka.producer();
  await producer.connect();
  log(`Połączono z Kafką (${BOOTSTRAP_SERVERS}). Wysyłam ${ORDER_COUNT} wiadomości na topic '${TOPIC}'.`);

  for (let i = 1; i <= ORDER_COUNT; i++) {
    const order = buildOrder();
    await producer.send({
      topic: TOPIC,
      messages: [{ key: order.orderId, value: JSON.stringify(order) }],
    });
    log(`Wysłano (${i}/${ORDER_COUNT}): ${JSON.stringify(order)}`);
    await sleep(INTERVAL_SECONDS);
  }

  await producer.disconnect();
  log("Zakończono wysyłanie.");
}

main().catch((err) => {
  console.error("Błąd producenta:", err);
  process.exit(1);
});
