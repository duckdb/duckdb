// Welcome to the easiest way to parse an SQL query :-)
// Compile the file like this:
//
// cc -I../ -L../ -lpg_query simple.c

#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

size_t testCount = 7;
const char* tests[] = {
  "SELECT 1",
  "SELECT * FROM x WHERE z = 2",
  "SELECT 5.41414",
  "SELECT $1",
  "SELECT ?",
  "SELECT 999999999999999999999::numeric/1000000000000000000000",
  "SELECT 4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"
};

int main() {
  PgQueryParseResult result;
  size_t i;

  for (i = 0; i < testCount; i++) {
    result = pg_query_parse(tests[i]);

    if (result.error) {
      printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    } else {
      printf("%s\n", result.parse_tree);
    }

    pg_query_free_parse_result(result);
  }

  return 0;
}
