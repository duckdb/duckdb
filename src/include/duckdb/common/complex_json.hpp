//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/complex_json.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <string>
#include "duckdb/common/exception.hpp"

namespace duckdb {

//! Custom struct to handle both strings and nested JSON objects
struct ComplexJSON {
	//! Constructor for string values
	ComplexJSON(const string &str, bool ignore_errors);
	//! Constructor for nested object values
	ComplexJSON(const unordered_map<string, ComplexJSON> &obj, bool ignore_errors);
	//! Basic empty constructor
	ComplexJSON();

	//! Adds Object to the underlying map, also sets the is_object flag to True
	void AddObject(const string &key, const ComplexJSON &object);
	//! Gets a ComplexJSON object from the map
	ComplexJSON GetObject(const string &key);
	//! Gets a string version of the underlying ComplexJSON object from the map
	string GetValue(const string &key) const;
	//! Recursive function for GetValue
	static string GetValueRecursive(const ComplexJSON &child);

private:
	//! Basic string value, in case this is the last value of a nested json
	string str_value;
	//! If this is a complex json a map of key/value
	unordered_map<string, ComplexJSON> obj_value;
	//! If this json is an object (i.e., map or not)
	bool is_object;
	//! If this json is malformed, should it be ignored, or should we throw an error during parsing.
	bool ignore_errors;
};

} // namespace duckdb
