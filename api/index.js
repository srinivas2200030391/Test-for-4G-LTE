const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const { MongoClient } = require("mongodb");

const app = express();
const upload = multer({ dest: "uploads/" });

// MongoDB connection URI and client setup
const uri =
  "mongodb://mongo:raDwXmWnTEqyJPUjFibZByeIqpKAScAS@switchback.proxy.rlwy.net:14376";

const client = new MongoClient(uri);

async function connectMongo() {
  try {
    await client.connect();
    console.log("Connected to MongoDB (native driver)");
  } catch (err) {
    console.error("MongoDB connection error:", err);
  }
}

// Connect once at app start
connectMongo();

// POST endpoint for CSV upload
app.post("/api/voltages/uploadCsv", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).send("No file uploaded.");

  const readings = [];
  fs.createReadStream(req.file.path)
    .pipe(csv(["timestamp", "voltage"]))
    .on("data", (row) => {
      readings.push({
        timestamp: new Date(row.timestamp),
        voltage: parseFloat(row.voltage),
      });
    })
    .on("end", async () => {
      try {
        const db = client.db("voltagedb");
        const collection = db.collection("VoltageReading"); // collection name

        if (readings.length === 0) {
          fs.unlinkSync(req.file.path);
          return res.status(400).send("CSV file is empty or invalid.");
        }

        await collection.insertMany(readings);
        fs.unlinkSync(req.file.path);
        res.send("CSV uploaded and data inserted.");
      } catch (err) {
        res.status(500).send("DB error: " + err.message);
      }
    })
    .on("error", (err) => {
      res.status(500).send("CSV parse error: " + err.message);
    });
});
// GET endpoint to fetch all voltage readings
app.get("/api/voltages", async (req, res) => {
  try {
    const db = client.db("voltagedb");
    const collection = db.collection("VoltageReading");

    const data = await collection.find({}).toArray();
    res.json(data); // Return the sweet data to the world 💌
  } catch (err) {
    res
      .status(500)
      .send("Failed to fetch voltage readings, my love: " + err.message);
  }
});

app.get("/", (req, res) => {
  res.send(
    "Welcome to the Voltage API! Use /api/voltages/uploadCsv to upload CSV files and / api / voltages to fetch readings."
  );
});
app.listen(3000, () => {
  console.log("Server running on port 3000");
});
