#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

string SelectionVector::ToString(idx_t count) {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for(idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Print(idx_t count) {
	Printer::Print(ToString(count));
}

}
