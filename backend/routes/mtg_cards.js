const express = require('express');
const router = express.Router();
const mtg_cards = require('../services/mtg_cards');

/* GET MTG Cards from the database */
router.get('/', async function(req, res, next) {
  try {
    res.json(await mtg_cards.getMultiple(req.query.page));
  } catch (err) {
    console.error(`Error while getting mtg_cards `, err.message);
    next(err);
  }
});

module.exports = router;