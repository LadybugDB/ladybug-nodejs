const { assert } = require("chai");
const { makeVector } = require("apache-arrow");

const itWhenCSRMetadataIsSupported = process.platform === "win32" ? it.skip : it;

function toNumbers(values) {
  return Array.from(values, Number);
}

function reconstructWithEdgeIds(csr) {
  const indptr = toNumbers(csr.indptr);
  const indices = toNumbers(csr.indices);
  const edgeIds = toNumbers(csr.edgeIds);
  const rows = [];
  for (let srcRowId = 0; srcRowId < indptr.length - 1; srcRowId++) {
    for (let idx = indptr[srcRowId]; idx < indptr[srcRowId + 1]; idx++) {
      rows.push([srcRowId, edgeIds[idx], indices[idx]]);
    }
  }
  return rows;
}

function reconstructWithoutEdgeIds(csr) {
  const indptr = toNumbers(csr.indptr);
  const indices = toNumbers(csr.indices);
  const rows = [];
  for (let srcRowId = 0; srcRowId < indptr.length - 1; srcRowId++) {
    for (let idx = indptr[srcRowId]; idx < indptr[srcRowId + 1]; idx++) {
      rows.push([srcRowId, indices[idx]]);
    }
  }
  return rows;
}

describe("Arrow query CSR", function () {
  itWhenCSRMetadataIsSupported("should expose zero-copy CSR arrays with relationship row ids", function () {
    const query = `
      MATCH (a:person)-[b:knows]->(c:person)
      RETURN a.rowid, b.rowid, c.rowid
    `;
    const rows = conn.querySync(query).getAllSync().map((row) => [
      row["a.rowid"],
      row["b.rowid"],
      row["c.rowid"],
    ]);

    const csr = conn.queryArrowSync(query, 8).csr();

    assert.instanceOf(csr.indptr, BigUint64Array);
    assert.instanceOf(csr.indices, BigUint64Array);
    assert.instanceOf(csr.edgeIds, BigUint64Array);
    assert.deepEqual(reconstructWithEdgeIds(csr), rows);
  });

  itWhenCSRMetadataIsSupported("should expose zero-copy CSR arrays without relationship row ids", function () {
    const query = `
      MATCH (a:person)-[:knows]->(c:person)
      RETURN a.rowid, c.rowid
    `;
    const rows = conn.querySync(query).getAllSync().map((row) => [
      row["a.rowid"],
      row["c.rowid"],
    ]);

    const csr = conn.queryArrowSync(query, 8).csr();

    assert.instanceOf(csr.indptr, BigUint64Array);
    assert.instanceOf(csr.indices, BigUint64Array);
    assert.isNull(csr.edgeIds);
    assert.deepEqual(reconstructWithoutEdgeIds(csr), rows);
  });

  itWhenCSRMetadataIsSupported("should support the async queryArrow API", async function () {
    const query = `
      MATCH (a:person)-[b:knows]->(c:person)
      RETURN a.rowid, b.rowid, c.rowid
    `;

    const result = await conn.queryArrow(query, 8);
    const csr = result.csr();

    assert.instanceOf(csr.indptr, BigUint64Array);
    assert.instanceOf(csr.indices, BigUint64Array);
    assert.instanceOf(csr.edgeIds, BigUint64Array);
  });

  itWhenCSRMetadataIsSupported("should allow CSR arrays to be wrapped by apache-arrow vectors", function () {
    const query = `
      MATCH (a:person)-[b:knows]->(c:person)
      RETURN a.rowid, b.rowid, c.rowid
    `;
    const csr = conn.queryArrowSync(query, 8).csr();

    const indptr = makeVector(csr.indptr);
    const indices = makeVector(csr.indices);
    const edgeIds = makeVector(csr.edgeIds);

    assert.equal(indptr.length, csr.indptr.length);
    assert.equal(indices.length, csr.indices.length);
    assert.equal(edgeIds.length, csr.edgeIds.length);
    assert.equal(indptr.type.toString(), "Uint64");
    assert.equal(indices.type.toString(), "Uint64");
    assert.equal(edgeIds.type.toString(), "Uint64");
  });

  it("should reject csr() for Arrow results without CSR metadata", function () {
    const result = conn.queryArrowSync("MATCH (a:person) RETURN a.fName", 8);

    assert.throws(
      () => result.csr(),
      /CSR export is only supported/
    );
  });
});
