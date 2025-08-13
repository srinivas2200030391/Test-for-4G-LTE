const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { MongoClient } = require("mongodb");
const kafka = require("kafka-node"); // 💡 Added Kafka

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

const uri =
  "mongodb://mongo:raDwXmWnTEqyJPUjFibZByeIqpKAScAS@switchback.proxy.rlwy.net:14376";

app.use(express.json());

// -----------------------------
// Kafka Setup 💌
// -----------------------------
const kafkaHost = "BROKER_IP:9092"; // Change BROKER_IP to your Kafka broker host
const kafkaTopic = "control-commands";

const kafkaClient = new kafka.KafkaClient({ kafkaHost });
const producer = new kafka.Producer(kafkaClient);

producer.on("ready", () => {
  console.log("[KAFKA] Producer ready");
});

producer.on("error", (err) => {
  console.error("[KAFKA] Producer error:", err);
});

// Helper to send Kafka event
function sendKafkaEvent(event) {
  const payloads = [{ topic: kafkaTopic, messages: JSON.stringify(event) }];
  producer.send(payloads, (err, data) => {
    if (err) console.error("[KAFKA] Send error:", err);
    else console.log("[KAFKA] Event sent:", data);
  });
}

// -----------------------------
// CSV Upload Route
// -----------------------------
app.post("/api/voltages/uploadCsv", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).send("No file uploaded");

  const readings = [];

  try {
    const csvStream = Readable.from(req.file.buffer.toString()).pipe(
      csv(["timestamp", "voltage"])
    );

    csvStream
      .on("data", (row) => {
        readings.push({
          timestamp: new Date(row.timestamp),
          voltage: parseFloat(row.voltage),
        });
      })
      .on("end", async () => {
        if (readings.length === 0) {
          return res.status(400).send("The CSV was empty");
        }

        const client = new MongoClient(uri);
        await client.connect();
        const db = client.db("voltagedb");
        const collection = db.collection("VoltageReading");

        await collection.insertMany(readings);
        await client.close();

        // 💌 Send Kafka event after successful insert
        sendKafkaEvent({ event: "new_data" });

        res.send("✨ CSV uploaded and saved");
      })
      .on("error", (err) => {
        res.status(500).send("CSV parsing error: " + err.message);
      });
  } catch (err) {
    res.status(500).send("Something went wrong: " + err.message);
  }
});

// -----------------------------
// Get All Readings
// -----------------------------
app.get("/api/voltages", async (req, res) => {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db("voltagedb");
    const data = await db.collection("VoltageReading").find({}).toArray();
    await client.close();

    res.json(data);
  } catch (err) {
    res.status(500).send("Couldn’t fetch readings: " + err.message);
  }
});

// -----------------------------
// Delete All Readings
// -----------------------------
app.delete("/api/voltages", async (req, res) => {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db("voltagedb");
    const collection = db.collection("VoltageReading");

    const result = await collection.deleteMany({});
    await client.close();

    res.send(`💀 All voltage readings deleted. Count: ${result.deletedCount}`);
  } catch (err) {
    res.status(500).send("Couldn’t delete the data: " + err.message);
  }
});
app.get("/api/voltages/new", async (req, res) => {
  try {
    const since = req.query.since;
    if (!since) {
      return res.status(400).send("Missing 'since' query parameter");
    }

    const sinceDate = new Date(since);
    if (isNaN(sinceDate)) {
      return res.status(400).send("Invalid date format for 'since'");
    }

    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db("voltagedb");
    const data = await db
      .collection("VoltageReading")
      .find({ timestamp: { $gt: sinceDate } })
      .toArray();
    await client.close();

    res.json(data);
  } catch (err) {
    res.status(500).send("Couldn’t fetch new readings: " + err.message);
  }
});

app.get("/", (req, res) => {
  res.send(
    "Welcome to the Voltage API! Upload your CSV at `/api/voltages/uploadCsv` and fetch data from `/api/voltages`."
  );
});

module.exports = app;

app.listen(3000, () => {
  console.log("Server running on port 3000");
});
