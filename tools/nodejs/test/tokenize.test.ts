import * as assert from 'assert';
import * as duckdb from '..';

describe('tokenize', function () {
  it('should return correct tokens for a single statement', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize('select 1;');
    assert.deepStrictEqual(output, {
      offsets: [0, 7, 8],
      types: [duckdb.TokenType.KEYWORD, duckdb.TokenType.NUMERIC_CONSTANT, duckdb.TokenType.OPERATOR]
    });
  });
  it('should return correct tokens for a multiple statements', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize('select 1; select 2;');
    assert.deepStrictEqual(output, {
      offsets: [0, 7, 8, 10, 17, 18],
      types: [
        duckdb.TokenType.KEYWORD, duckdb.TokenType.NUMERIC_CONSTANT, duckdb.TokenType.OPERATOR,
        duckdb.TokenType.KEYWORD, duckdb.TokenType.NUMERIC_CONSTANT, duckdb.TokenType.OPERATOR
      ]
    });
  });
  it('should return no tokens for an empty string', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize('');
    assert.deepStrictEqual(output, {
      offsets: [],
      types: []
    });
  });
  it('should handle quoted semicolons in string constants', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize(`select ';';`);
    assert.deepStrictEqual(output, {
      offsets: [0, 7, 10],
      types: [duckdb.TokenType.KEYWORD, duckdb.TokenType.STRING_CONSTANT, duckdb.TokenType.OPERATOR]
    });
  });
  it('should handle quoted semicolons in identifiers', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize(`from ";";`);
    assert.deepStrictEqual(output, {
      offsets: [0, 5, 8],
      types: [duckdb.TokenType.KEYWORD, duckdb.TokenType.IDENTIFIER, duckdb.TokenType.OPERATOR]
    });
  });
  it('should handle comments', function () {
    const db = new duckdb.Database(':memory:');
    const output = db.tokenize(`select /* comment */ 1`);
    // Note that the tokenizer doesn't return tokens for comments.
    assert.deepStrictEqual(output, {
      offsets: [0, 21],
      types: [duckdb.TokenType.KEYWORD, duckdb.TokenType.NUMERIC_CONSTANT]
    });
  });
});
