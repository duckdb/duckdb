rm -f src/duckdb.cpp src/duckdb.h src/duckdb.o src/duckdbr.o src/duckdb.so src/Makevars
R -q -e "if (is.element('duckdb', installed.packages()[,1])) { remove.packages('duckdb') }"
