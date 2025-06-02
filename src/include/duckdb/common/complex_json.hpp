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
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

//! Custom struct to handle both strings and nested JSON objects
struct ComplexJSON {
	//! Constructor for string values
	explicit ComplexJSON(const string &str);
	//! Basic empty constructor
	ComplexJSON();
	//! Adds Object to the underlying map, also sets the is_object flag to True
	void AddObject(const string &key, unique_ptr<ComplexJSON> object);
	//! Gets a ComplexJSON object from the map
	ComplexJSON &GetObject(const string &key);
	//! Gets a string version of the underlying ComplexJSON object from the map
	string GetValue(const string &key) const;
	//! Recursive function for GetValue
	static string GetValueRecursive(const ComplexJSON &child);
	//! Flattens this json to a top level key -> nested json
	unordered_map<string, string> Flatten() const;

private:
	//! Basic string value, in case this is the last value of a nested json
	string str_value;
	//! If this is a complex json a map of key/value
	unordered_map<string, unique_ptr<ComplexJSON>> obj_value;
	//! If this json is an object (i.e., map or not)
	bool is_object;
};

} // namespace duckdb
