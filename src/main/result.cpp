
#include "main/result.hpp"

using namespace duckdb;
using namespace std;

DuckDBResult::DuckDBResult() : success(true) {}

DuckDBResult::DuckDBResult(std::string error) : success(false), error(error) {}

void DuckDBResult::Print() {
	if (success) {
		for (auto &type : types()) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf(" [ %zu ]\n", size());
		for (size_t j = 0; j < size(); j++) {
			for (size_t i = 0; i < column_count(); i++) {
				printf("%s\t", collection.GetValue(i, j).ToString().c_str());
			}
			printf("\n");
		}
		printf("\n");
	} else {
		printf("Query Error: %s\n", error.c_str());
	}
}
