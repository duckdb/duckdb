#include <pg_query.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "parse_tests.c"

int main() {
  size_t i;
  bool ret_code = 0;

  for (i = 0; i < testsLength; i += 2) {
    PgQueryParseResult result = pg_query_parse(tests[i]);

    if (strcmp(result.parse_tree, tests[i + 1]) == 0) {
      printf(".");
    } else {
      ret_code = -1;
      printf("INVALID result for \"%s\"\nexpected: %s\nactual: %s\n", tests[i], tests[i + 1], result.parse_tree);
    }

    pg_query_free_parse_result(result);
  }

  printf("\n");

  return ret_code;
}
