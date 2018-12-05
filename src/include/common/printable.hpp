//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/printable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

class Deserializer;
class Serializer;

//! Printable is an abstract class that represents an object that can be
//! converted to a string for logging and debugging purposes.
class Printable {
public:
	virtual ~Printable(){};

	//! Convert the object to a string
	virtual string ToString() const = 0;

	//! Print the object to stderr
	void Print();
};
} // namespace duckdb
