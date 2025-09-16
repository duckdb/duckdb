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
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

enum class ComplexJSONType : uint8_t { VALUE = 0, OBJECT = 1, ARRAY = 2, NULL_VALUE = 3 };

//! Custom struct to handle both strings and nested JSON objects
struct ComplexJSON {
	//! Constructor for string values
	explicit ComplexJSON(const string &str);
	//! Basic empty constructor
	explicit ComplexJSON(ComplexJSONType type = ComplexJSONType::VALUE);
	//! Adds entry to the underlying map, also sets the type to OBJECT
	void AddObjectEntry(const string &key, unique_ptr<ComplexJSON> object);
	//! Adds element to the underlying list, also sets the type to ARRAY
	void AddArrayElement(unique_ptr<ComplexJSON> object);
	//! Gets a ComplexJSON object from the map
	ComplexJSON &GetObject(const string &key);
	//! Gets a ComplexJSON element from the list
	ComplexJSON &GetArrayElement(const idx_t &index);
	//! Gets a string version of the underlying ComplexJSON object from the map
	string GetValue(const string &key) const;
	//! Gets a string version of the underlying ComplexJSON array from the list
	string GetValue(const idx_t &index) const;
	//! Recursive function for GetValue
	static string GetValueRecursive(const ComplexJSON &child);
	//! Flattens this json to a top level key -> nested json
	unordered_map<string, string> Flatten() const;

private:
	//! Basic string value, in case this is the last value of a nested json
	string str_value;
	//! If this is a json object a map of key/value
	unordered_map<string, unique_ptr<ComplexJSON>> obj_value;
	//! If this is a json array a list of values
	vector<unique_ptr<ComplexJSON>> arr_value;
	//! If this json is an object (i.e., map or not)
	ComplexJSONType type;
};

} // namespace duckdb
