
#pragma once

#include "common/types/vector.hpp"

namespace duckdb {

class VectorOperations {
  public:
	static void Add(Vector &left, Vector &right, Vector &result);
	static void Subtract(Vector &left, Vector &right, Vector &result);
	static void Multiply(Vector &left, Vector &right, Vector &result);
	static void Divide(Vector &left, Vector &right, Vector &result);
};
}
