#include "duckdb/common/complex_json.hpp"

namespace duckdb {
ComplexJSON::ComplexJSON(const string &str, bool ignore_errors)
    : str_value(str), is_object(false), ignore_errors(ignore_errors) {
}

ComplexJSON::ComplexJSON() : is_object(false), ignore_errors(false) {}

void ComplexJSON::AddObject(const string &key, unique_ptr<ComplexJSON> object) {
	is_object = true;
	obj_value[key] = std::move(object);
}

ComplexJSON &ComplexJSON::GetObject(const string &key) {
	if (is_object) {
		if (obj_value.find(key) == obj_value.end()) {
			throw InvalidInputException("Complex JSON Key not found");
		}
		return *obj_value[key];
	}
	throw InvalidInputException("ComplexJson is not an object");
}
} // namespace duckdb
