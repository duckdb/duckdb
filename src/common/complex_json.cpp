#include "duckdb/common/complex_json.hpp"

namespace duckdb {
//! Constructor for string values
ComplexJSON::ComplexJSON(const string &str) : str_value(str), is_object(false) {
}

//! Constructor for nested object values
ComplexJSON::ComplexJSON(const unordered_map<string, ComplexJSON> &obj) : obj_value(obj), is_object(true) {
}

ComplexJSON::ComplexJSON() : is_object(false) {};

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
