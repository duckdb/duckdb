//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/printable.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/helper.hpp"

namespace duckdb {

//! Printable is an abstract class that represents an object that can be
//! converted to a string for logging and debugging purposes.
class Printable {
  public:
	virtual ~Printable(){};

	//! Convert the object to a string
	virtual std::string ToString() const = 0;

	//! Print the object to stderr
	void Print();
};
} // namespace duckdb
