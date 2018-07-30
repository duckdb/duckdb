
#pragma once

#include <string>

#include "common/helper.hpp"

namespace duckdb {
class Printable {
  public:
	virtual ~Printable(){};

	/** @brief Get the info about the object. */
	virtual std::string ToString() const = 0;

	void Print();
};
} // namespace duckdb
