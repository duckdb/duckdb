#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  PgQueryParseResult result;

  result = pg_query_parse("INSERT FROM DOES NOT WORK");

  if (result.error) {
    printf("error: %s at location %d (%s in %s:%d)\n", result.error->message,
           result.error->cursorpos, result.error->funcname, result.error->filename, result.error->lineno);
  } else {
    printf("%s\n", result.parse_tree);
  }

  pg_query_free_parse_result(result);

  return 0;
}
