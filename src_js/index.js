"use strict";

const Connection = require("./connection.js");
const Database = require("./database.js");
const PreparedStatement = require("./prepared_statement.js");
const QueryResult = require("./query_result.js");
const { ArrowQueryResult } = require("./query_result.js");

function json(value) {
  const stringValue = typeof value === "string" ? value : JSON.stringify(value);
  return Object.freeze(
    Object.defineProperties({}, {
      _lbugType: { value: "JSON", enumerable: false },
      value: { value: stringValue, enumerable: false },
    })
  );
}

module.exports = {
  Connection,
  Database,
  PreparedStatement,
  QueryResult,
  ArrowQueryResult,
  json,
  get VERSION() {
    return Database.getVersion();
  },
  get STORAGE_VERSION() {
    return Database.getStorageVersion();
  },
};
