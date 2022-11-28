const express = require("express");
var cors = require('cors')
const app = express();
const port = 3001;
const mtg_cards = require("./routes/mtg_cards");
app.use(express.json());
app.use(
  express.urlencoded({
    extended: true,
  })
);
app.use(cors())

// Backend routes

app.get("/", (req, res) => {
  res.json({ message: "ok" });
});

app.use("/mtg_cards", mtg_cards);
/* Error handler middleware */
app.use((err, req, res, next) => {
  const statusCode = err.statusCode || 500;
  console.error(err.message, err.stack);
  res.status(statusCode).json({ message: err.message });
  return;
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});