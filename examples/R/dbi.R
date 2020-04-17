# Install the DuckDB R package like so: R CMD INSTALL tools/rpkg
# Or with install.packages: install.packages("duckdb", 
#                                            repos=c("http://download.duckdb.org/alias/master/rstats/", 
#                                                    "http://cran.rstudio.com"))

# The DuckDB R interface follows the R DBI specification
library("DBI")

# create a DuckDB connection, either as a temporary in-memory database (default) or with a file 
con <- dbConnect(duckdb::duckdb(), ":memory:")

# write a data.frame to the database
dbWriteTable(con, "iris", iris)

# now we have a table called mtcars with the contents of the data frame
dbListTables(con)
dbListFields(con, "iris")

# we can read the entire table back into R
iris2 <- dbReadTable(con, "iris")

# we can also run arbitray SQL commands on this table
iris3 <- dbGetQuery(con, 'SELECT "Species", MIN("Sepal.Width") FROM iris GROUP BY "Species"')

# we can use prepared statements to parameterize queries:
dbGetQuery(con, 'SELECT COUNT(*) FROM iris WHERE "Sepal.Length" > ? AND "Species" = ?', 7, "virginica")

# delete the table again
dbRemoveTable(con, "iris")

# close the connection
dbDisconnect(con)
