const db = require('./db');
const helper = require('../helper');

async function getData(){
  const rows = await db.query(
    `SELECT DISTINCT * 
    FROM mtg_cards`
  );
  data = helper.emptyOrRows(rows)
  return {
    data
  }
}

module.exports = {
  getData
  }