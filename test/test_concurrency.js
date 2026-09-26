const { assert } = require("chai");

describe("Concurrent query execution within a single connection", function () {
  it("should dispatch multiple queries concurrently with query strings", async function () {
    const queryResults = await Promise.all([
      conn.query("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 2 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 3 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 5 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 7 RETURN a.isStudent;"),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAll())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
  });

  it("should dispatch multiple queries concurrently with prepared statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $id RETURN a.isStudent;"
    );
    const queryResults = await Promise.all([
      conn.execute(preparedStatement, { id: 0 }),
      conn.execute(preparedStatement, { id: 2 }),
      conn.execute(preparedStatement, { id: 3 }),
      conn.execute(preparedStatement, { id: 5 }),
      conn.execute(preparedStatement, { id: 7 }),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAll())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
  });
});

describe("Concurrent query execution across multiple connections", function () {
  it("should dispatch multiple queries concurrently on different connections", async function () {
    const connections = [];
    for (let i = 0; i < 5; i++) {
      connections.push(new lbug.Connection(db));
    }
    const queryResults = await Promise.all([
      connections[0].query(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"
      ),
      connections[1].query(
        "MATCH (a:person) WHERE a.ID = 2 RETURN a.isStudent;"
      ),
      connections[2].query(
        "MATCH (a:person) WHERE a.ID = 3 RETURN a.isStudent;"
      ),
      connections[3].query(
        "MATCH (a:person) WHERE a.ID = 5 RETURN a.isStudent;"
      ),
      connections[4].query(
        "MATCH (a:person) WHERE a.ID = 7 RETURN a.isStudent;"
      ),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAll())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
  });
});

describe("Concurrent initialization on a fresh connection (issue #24)", function () {
  it("should handle concurrent queries as the very first use without explicit init", async function () {
    // Regression test for https://github.com/LadybugDB/ladybug-nodejs/issues/24.
    // Each iteration uses a fresh in-memory database + connection and fires
    // two queries concurrently as the first operation. Before the fix, the
    // lazy-init race in Connection.init() crashed the process (SIGSEGV).
    // bufferPoolSize 64 MB, maxDBSize 1 GB (the default maxDBSize asks for
    // an 8 TB mmap, which is too large for some environments).
    for (let i = 0; i < 50; i++) {
      const memDb = new lbug.Database(
        ":memory:",
        64 * 1024 * 1024,
        true,
        false,
        1024 * 1024 * 1024
      );
      const freshConn = new lbug.Connection(memDb);
      const queryResults = await Promise.all([
        freshConn.query("RETURN 1 AS x"),
        freshConn.query("RETURN 2 AS x"),
      ]);
      const rows = await Promise.all(
        queryResults.map((queryResult) => queryResult.getAll())
      );
      assert.deepEqual(rows[0], [{ x: 1 }]);
      assert.deepEqual(rows[1], [{ x: 2 }]);
      for (const queryResult of queryResults) {
        queryResult.close();
      }
      await freshConn.close();
      await memDb.close();
    }
  });

  it("should handle concurrent init() calls on a fresh connection", async function () {
    const memDb = new lbug.Database(
      ":memory:",
      64 * 1024 * 1024,
      true,
      false,
      1024 * 1024 * 1024
    );
    const freshConn = new lbug.Connection(memDb);
    await Promise.all([freshConn.init(), freshConn.init()]);
    const queryResult = await freshConn.query("RETURN 1 AS x");
    assert.deepEqual(await queryResult.getAll(), [{ x: 1 }]);
    queryResult.close();
    await freshConn.close();
    await memDb.close();
  });
});
