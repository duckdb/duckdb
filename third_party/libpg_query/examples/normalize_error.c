#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  PgQueryNormalizeResult result;

  result = pg_query_normalize("SELECT $$$");

  if (result.error) {
    printf("error: %s at location %d (%s:%d)\n", result.error->message,
           result.error->cursorpos, result.error->filename, result.error->lineno);
  } else {
    printf("%s\n", result.normalized_query);
  }

  pg_query_free_normalize_result(result);

  return 0;
}
