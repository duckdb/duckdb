//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/complex_json.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
//! Custom struct to handle both strings and nested JSON objects
struct ComplexJSON {
	//! Constructor for string values
	explicit ComplexJSON(const string &str) : str_value(str), is_object(false) {
	}

	//! Constructor for nested object values
	explicit ComplexJSON(const unordered_map<string, ComplexJSON> &obj) : obj_value(obj), is_object(true) {
	}

	ComplexJSON() : is_object(false) {};

	//! Adds Object
	void AddObject(const string &key, const ComplexJSON object) {
		is_object = true;
		obj_value[key] = object;
	}
	ComplexJSON GetObject(const string &key) {
		if (is_object) {
			return obj_value[key];
		}
	}
	string str_value;
	unordered_map<string, ComplexJSON> obj_value;
	bool is_object;
};

} // namespace duckdb
