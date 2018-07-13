#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  PgQueryNormalizeResult result;

  result = pg_query_normalize("SELECT 1");

  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
  } else {
    printf("%s\n", result.normalized_query);
  }

  pg_query_free_normalize_result(result);

  return 0;
}
