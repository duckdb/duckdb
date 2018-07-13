const char* tests[] = {
  "SELECT 1",
  "SELECT ?",
  "ALTER ROLE postgres LOGIN SUPERUSER PASSWORD 'xyz'",
  "ALTER ROLE postgres LOGIN SUPERUSER PASSWORD ?"
};

size_t testsLength = __LINE__ - 4;
