#include <pg_query.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "normalize_tests.c"

int main() {
  size_t i;
  bool ret_code = 0;

  for (i = 0; i < testsLength; i += 2) {
    PgQueryNormalizeResult result = pg_query_normalize(tests[i]);

    if (strcmp(result.normalized_query, tests[i + 1]) == 0) {
      printf(".");
    } else {
      ret_code = -1;
      printf("INVALID result for \"%s\"\nexpected: %s\nactual: %s\n", tests[i], tests[i + 1], result.normalized_query);
    }

    pg_query_free_normalize_result(result);
  }

  printf("\n");

  return ret_code;
}
