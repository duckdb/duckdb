.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -f *.class
	rm -rf target

duckdb:
	BUILD_JDBC=1 make -C ../..

main:
	javac DuckDBExample.java
	java -cp .:../../build/release/tools/jdbc/duckdb_jdbc.jar DuckDBExample

maven:
	mvn compile exec:java -Dexec.mainClass="DuckDBExample"