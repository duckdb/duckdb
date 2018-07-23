
#pragma once

#include <vector>

#include "common/internal_types.hpp"

namespace duckdb {
class DataColumn {
  public:
	DataColumn(TypeId type) : data(nullptr), type(type) {}

	void *data;
	TypeId type;
};
}
