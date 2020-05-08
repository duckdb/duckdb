.PHONY: duckdb clean main

all: duckdb main

clean:
	rm *.class

duckdb:
	make -C ../.. jdbc

main:
	javac DuckDBExample.java
	java -cp .:../../build/release/tools/jdbc/duckdb_jdbc.jar DuckDBExample

