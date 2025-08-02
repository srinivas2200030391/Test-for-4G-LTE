const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { MongoClient } = require("mongodb");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

const uri =
  "mongodb://mongo:raDwXmWnTEqyJPUjFibZByeIqpKAScAS@switchback.proxy.rlwy.net:14376";

app.use(express.json());

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

        res.send(
          "✨ CSV uploaded and saved"
        );
      })
      .on("error", (err) => {
        res.status(500).send("CSV parsing error, sugarplum: " + err.message);
      });
  } catch (err) {
    res.status(500).send("Something went wrong: " + err.message);
  }
});

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

app.get("/", (req, res) => {
  res.send(
    "💖 Welcome to the Voltage API! Upload your CSV at `/api/voltages/uploadCsv` and fetch data from `/api/voltages`. Stay electric, darling ⚡"
  );
});module.exports = app; // export for Vercel

app.listen(3000, () => {
  console.log("Server running on port 3000");
});
