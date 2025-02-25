#include "duckdb/common/complex_json.hpp"

namespace duckdb {
ComplexJSON::ComplexJSON(const string &str, bool ignore_errors)
    : str_value(str), is_object(false), ignore_errors(ignore_errors) {
}

ComplexJSON::ComplexJSON(const unordered_map<string, ComplexJSON> &obj, bool ignore_errors)
    : obj_value(obj), is_object(true), ignore_errors(ignore_errors) {
}

ComplexJSON::ComplexJSON() : is_object(false), ignore_errors(false) {};

//! Adds Object
void ComplexJSON::AddObject(const string &key, const ComplexJSON &object) {
	is_object = true;
	obj_value[key] = object;
}
ComplexJSON ComplexJSON::GetObject(const string &key) {
	if (is_object) {
		if (obj_value.find(key) == obj_value.end()) {
			return ComplexJSON();
		}
		return obj_value[key];
	} else {
		throw InvalidInputException("ComplexJson is not an object");
	}
}
} // namespace duckdb
