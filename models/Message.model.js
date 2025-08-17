const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
  timestamp: { type: Date, required: true },
  message: { type: String, required: true }
});

module.exports = mongoose.model('Message', MessageSchema);
