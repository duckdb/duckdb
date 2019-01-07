#include "main/result.hpp"

using namespace duckdb;
using namespace std;

DuckDBResult::DuckDBResult() : success(true) {
}

DuckDBResult::DuckDBResult(string error) : success(false), error(error) {
}

void DuckDBResult::Print() {
	if (success) {
		for (auto &name : names) {
			printf("%s\t", name.c_str());
		}
		printf(" [ %zu ]\n", size());
		for (auto &type : types()) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf("\n");
		for (size_t j = 0; j < size(); j++) {
			for (size_t i = 0; i < column_count(); i++) {
				printf("%s\t", collection.GetValue(i, j).ToString().c_str());
			}
			printf("\n");
		}
		printf("\n");
	} else {
		fprintf(stderr, "Query Error: %s\n", error.c_str());
	}
}

bool DuckDBResult::Equals(DuckDBResult *other) {
	if (!other) {
		return false;
	}
	// first compare the success state of the results
	if (success != other->success) {
		return false;
	}
	if (!success) {
		return error == other->error;
	}
	// compare names
	if (names.size() != other->names.size()) {
		return false;
	}
	for (size_t i = 0; i < names.size(); i++) {
		if (names[i] != other->names[i]) {
			return false;
		}
	}
	// now compare the types and amount of values
	if (size() != other->size()) {
		return false;
	}
	if (column_count() != other->column_count()) {
		return false;
	}
	auto &ltypes = types(), &rtypes = other->types();
	if (ltypes.size() != rtypes.size()) {
		return false;
	}
	for (size_t i = 0; i < ltypes.size(); i++) {
		if (ltypes[i] != rtypes[i]) {
			return false;
		}
	}
	// now compare the actual values
	for (size_t row = 0; row < collection.count; row++) {
		for (size_t col = 0; col < collection.column_count(); col++) {
			auto lvalue = collection.GetValue(col, row);
			auto rvalue = other->collection.GetValue(col, row);
			if (lvalue != rvalue) {
				return false;
			}
		}
	}
	return true;
}
