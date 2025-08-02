const mongoose = require('mongoose');

const voltageReadingSchema = new mongoose.Schema({
  timestamp: { type: Date, required: true },
  voltage: { type: Number, required: true }
});

module.exports = mongoose.model('VoltageReading', voltageReadingSchema);
