# empty statement gives an error

    rapi_prepare: No statements to execute

# multiple statements can be used in one call

    Code
      DBI::dbGetQuery(con, paste("DROP TABLE IF EXISTS integers;", query))
    Output
         i
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7  6
      8  7
      9  8
      10 9

# statements can be splitted apart correctly

    Code
      DBI::dbGetQuery(con, a <- paste("--Multistatement testing; testing",
        "/*  test;   ", "--test;", ";test */", "create table temp_test as ", "select",
        "'testing_temp;' as temp_col", ";", "select * from temp_test;", sep = "\n"))
    Output
             temp_col
      1 testing_temp;

