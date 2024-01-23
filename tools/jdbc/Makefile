ifeq ($(OS),Windows_NT)
	# windows is weird
	SEP=";"
	JARS=tools/jdbc
else
	SEP=":"
	JARS=build/debug/tools/jdbc
endif

JAR=$(JARS)/duckdb_jdbc.jar
TEST_JAR=$(JARS)/duckdb_jdbc_tests.jar
CP=$(JAR)$(SEP)$(TEST_JAR)

test_debug: ../../$(JAR) ../../$(TEST_JAR)
	cd ../.. && java -cp $(CP) org.duckdb.test.TestDuckDBJDBC

test_release: ../../$(subst debug,release,$(JAR)) ../../$(subst debug,release,$(TEST_JAR))
	cd ../.. && java -cp $(subst debug,release,$(CP)) org.duckdb.test.TestDuckDBJDBC
