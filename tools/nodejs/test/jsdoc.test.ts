/**
 * Intended to be similar to stubtest for python
 */

import * as duckdb from "..";
// @ts-ignore
import jsdoc from "jsdoc3-parser";
const { expect } = require('chai');
const { promisify } = require('util');

function lastDot(string: string) {
  if (string.endsWith(')')) {
    string = string.substring(0, string.length - 1);
  }
  const array = string.split('.');
  return array[array.length - 1];
}

export interface Node {
  undocumented: boolean;
  name: string;
  memberof: string;
  longname: string;
  scope: string;
}
describe("JSDoc contains all methods", () => {
  let docs: Node[];
  before(async () => {
    docs = await promisify(jsdoc)(require.resolve("../lib/duckdb"));
  })

  function checkDocs(obj: object, scope: string) {
    const symbols = Object.getOwnPropertySymbols(obj).map(i => lastDot(i.toString()));
    const expected = Object
      .getOwnPropertyNames(obj)
      .concat(symbols)
      .sort()
      .filter(name => name !== 'constructor' && name !== 'default');

    const actual = docs
      .filter((node) => node.memberof === scope && !node.undocumented && node.name !== 'sql')  // `sql` is a field, so won't show up in the prototype
      .map((node) => lastDot(node.name))
      .sort();

    expect(expected).to.deep.equals(actual, 'items missing from documentation');
  }

  for (const clazz of ['Database', 'QueryResult', 'Connection', 'Statement']) {
    it(clazz, () => {
      // @ts-ignore
      let clazzObj = duckdb[clazz];
      checkDocs(clazzObj.prototype, `module:duckdb~${clazz}`);
    });
  }

  it('module root', () => {
    checkDocs(duckdb, 'module:duckdb');
  });
});
