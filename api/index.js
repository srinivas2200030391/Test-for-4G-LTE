// const express = require("express");
// const multer = require("multer");
// const csv = require("csv-parser");
// const { Readable } = require("stream");
// const { MongoClient } = require("mongodb");
// const { Kafka } = require("kafkajs");
// const fs = require("fs");
// const path = require("path");

// const app = express();
// const upload = multer({ storage: multer.memoryStorage() });

// const uri =
//   "mongodb://mongo:raDwXmWnTEqyJPUjFibZByeIqpKAScAS@switchback.proxy.rlwy.net:14376";

// // -----------------------------
// // Kafka Setup with SSL 💌
// // -----------------------------
// const kafkaBrokers = ["kafka-36cdd7ab-cronack-2088.e.aivencloud.com:19352"];
// const certsDir = path.join(__dirname, "certs");
// console.log(certsDir);
// const kafka = new Kafka({
//   clientId: "express-server",
//   brokers: kafkaBrokers,
//   ssl: {
//     rejectUnauthorized: true,
//     ca: [fs.readFileSync(path.join(certsDir, "ca.pem"), "utf-8")],
//     cert: fs.readFileSync(path.join(certsDir, "service.cert"), "utf-8"),
//     key: fs.readFileSync(path.join(certsDir, "service.key"), "utf-8"),
//   },
// });

// const kafkaTopic = "control-commands";
// let producer;

// async function initKafka() {
//   try {
//     producer = kafka.producer();
//     await producer.connect();
//     console.log("[KAFKA] Producer connected ✅");
//   } catch (error) {
//     console.error("[KAFKA] Producer connection error ❌:", error);
//   }
// }

// async function sendKafkaEvent(event) {
//   if (!producer) {
//     console.warn("[KAFKA] Producer not ready, skipping send");
//     return;
//   }
//   try {
//     await producer.send({
//       topic: kafkaTopic,
//       messages: [{ value: JSON.stringify(event) }],
//     });
//     console.log("[KAFKA] Event sent 📡:", event);
//   } catch (err) {
//     console.error("[KAFKA] Send error ❌:", err);
//   }
// }

// // -----------------------------
// // CSV Upload Route
// // -----------------------------
// app.post("/api/voltages/uploadCsv", upload.single("file"), async (req, res) => {
//   if (!req.file) return res.status(400).send("No file uploaded");

//   const readings = [];

//   try {
//     const csvStream = Readable.from(req.file.buffer.toString()).pipe(
//       csv(["timestamp", "voltage"])
//     );

//     csvStream
//       .on("data", (row) => {
//         readings.push({
//           timestamp: new Date(row.timestamp),
//           voltage: parseFloat(row.voltage),
//         });
//       })
//       .on("end", async () => {
//         if (readings.length === 0) {
//           return res.status(400).send("The CSV was empty");
//         }

//         const client = new MongoClient(uri);
//         await client.connect();
//         const db = client.db("voltagedb");
//         const collection = db.collection("VoltageReading");

//         await collection.insertMany(readings);
//         await client.close();

//         // 💌 Send Kafka event after successful insert
//         await sendKafkaEvent({ event: "new_data" });

//         res.send("✨ CSV uploaded and saved, Kafka notified securely!");
//       })
//       .on("error", (err) => {
//         res.status(500).send("CSV parsing error: " + err.message);
//       });
//   } catch (err) {
//     res.status(500).send("Something went wrong: " + err.message);
//   }
// });

// // -----------------------------
// // Get All Readings
// // -----------------------------
// app.get("/api/voltages", async (req, res) => {
//   try {
//     const client = new MongoClient(uri);
//     await client.connect();
//     const db = client.db("voltagedb");
//     const data = await db.collection("VoltageReading").find({}).toArray();
//     await client.close();

//     res.json(data);
//   } catch (err) {
//     res.status(500).send("Couldn’t fetch readings: " + err.message);
//   }
// });

// // -----------------------------
// // Get Only New Readings
// // -----------------------------
// app.get("/api/voltages/new", async (req, res) => {
//   try {
//     const since = req.query.since;
//     if (!since) {
//       return res.status(400).send("Missing 'since' query parameter");
//     }

//     const sinceDate = new Date(since);
//     if (isNaN(sinceDate)) {
//       return res.status(400).send("Invalid date format for 'since'");
//     }

//     const client = new MongoClient(uri);
//     await client.connect();
//     const db = client.db("voltagedb");
//     const data = await db
//       .collection("VoltageReading")
//       .find({ timestamp: { $gt: sinceDate } })
//       .toArray();
//     await client.close();

//     res.json(data);
//   } catch (err) {
//     res.status(500).send("Couldn’t fetch new readings: " + err.message);
//   }
// });

// // -----------------------------
// // Delete All Readings
// // -----------------------------
// app.delete("/api/voltages", async (req, res) => {
//   try {
//     const client = new MongoClient(uri);
//     await client.connect();
//     const db = client.db("voltagedb");
//     const collection = db.collection("VoltageReading");

//     const result = await collection.deleteMany({});
//     await client.close();

//     res.send(`💀 All voltage readings deleted. Count: ${result.deletedCount}`);
//   } catch (err) {
//     res.status(500).send("Couldn’t delete the data: " + err.message);
//   }
// });

// app.get("/", (req, res) => {
//   res.send(
//     "Welcome to the Voltage API! Upload your CSV at `/api/voltages/uploadCsv` and fetch data from `/api/voltages`."
//   );
// });

// // -----------------------------
// // Start Server + Kafka
// // -----------------------------
// app.listen(3000, async () => {
//   console.log("🚀 Server running on port 3000");
//   await initKafka();
// });

const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { MongoClient } = require("mongodb");
const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// -----------------------------
// MongoDB Connection URI
// -----------------------------
const uri =
  "mongodb://mongo:raDwXmWnTEqyJPUjFibZByeIqpKAScAS@switchback.proxy.rlwy.net:14376";

// -----------------------------
// Kafka Setup with SSL 💌
// -----------------------------
const kafkaBrokers = ["kafka-36cdd7ab-cronack-2088.e.aivencloud.com:19352"];
const certsDir = path.join(__dirname, "certs"); // inside /api

const kafka = new Kafka({
  clientId: "express-server",
  brokers: kafkaBrokers,
  ssl: {
    rejectUnauthorized: true,
    ca: [fs.readFileSync(path.join(certsDir, "ca.pem"), "utf-8")],
    cert: fs.readFileSync(path.join(certsDir, "service.cert"), "utf-8"),
    key: fs.readFileSync(path.join(certsDir, "service.key"), "utf-8"),
  },
});

const kafkaTopic = "control-commands";

// -----------------------------
// Kafka Send (per-request connection)
// -----------------------------
async function sendKafkaEvent(event) {
  try {
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic: kafkaTopic,
      messages: [{ value: JSON.stringify(event) }],
    });
    console.log("[KAFKA] Event sent 📡:", event);
    await producer.disconnect();
  } catch (err) {
    console.error("[KAFKA] Send error ❌:", err);
  }
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
        await sendKafkaEvent({ event: "new_data" });

        res.send("✨ CSV uploaded and saved, Kafka notified securely!");
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
// Get Only New Readings
// -----------------------------
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

// -----------------------------
// Insert a New Message & Send Kafka Event
// -----------------------------
app.post("/api/messages", express.json(), async (req, res) => {
  const { message, timestamp } = req.body;
  if (!message || !timestamp) {
    return res.status(400).send("Missing 'message' or 'timestamp'");
  }

  try {
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db("voltagedb"); // you may want a separate DB, e.g. "messagedb"
    const collection = db.collection("Message");

    // Save the new message (timestamp as Date)
    const doc = { timestamp: new Date(timestamp), message: String(message) };
    const result = await collection.insertOne(doc);
    await client.close();

    // 💌 Send Kafka event after a successful insert
    await sendKafkaEvent({ event: "new_message" });

    res.status(201).send("Message inserted and Kafka notified!");
  } catch (err) {
    res.status(500).send("Couldn’t insert message: " + err.message);
  }
});

app.get("/api/messages/new", async (req, res) => {
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
      .collection("Message")
      .find({ timestamp: { $gt: sinceDate } })
      .toArray();
    await client.close();

    res.json(data);
  } catch (err) {
    res.status(500).send("Couldn’t fetch new readings: " + err.message);
  }
});

app.delete("/api/messages", async (req, res) => {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db("voltagedb");
    const collection = db.collection("Message");

    const result = await collection.deleteMany({});
    await client.close();

    res.send(`💀 All messages deleted. Count: ${result.deletedCount}`);
  } catch (err) {
    res.status(500).send("Couldn’t delete the data: " + err.message);
  }
});

app.get("/", (req, res) => {
  res.send(
    "Welcome to the Voltage API! Upload your CSV at `/api/voltages/uploadCsv` and fetch data from `/api/voltages`."
  );
});

if (require.main === module) {
  app.listen(3000, () => {
    console.log("🚀 Local server running on http://localhost:3000");
  });
}

module.exports = app; // for Vercel
