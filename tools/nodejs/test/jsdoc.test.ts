/**
 * Intended to be similar to stubtest for python
 */

var duckdb = require("..");
var assert = require("assert");
var jsdoc = require("jsdoc");

describe("JSDoc", () => {
  it("JSDoc contains all methods", (done) => {
    const docs = jsdoc(require.resolve(".."));

    expect(
      docs
        .filter((node) => node.name.includes("Database"))
        .map((node) => node.name)
    ).toEqual(
      docs
        .filter((node) => node.name.includes("Database"))
        .map((node) => node.start)
    );
  });
});
