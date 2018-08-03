
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/types/vector_operations.hpp"

#include "storage/data_column.hpp"
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

void DataColumn::AddData(Vector &data) {
	if (data.type != column.type) {
		throw CatalogException("Mismatch in column type");
	}
	// update the statistics of the table
	Value min = VectorOperations::Min(data);
	Value max = VectorOperations::Max(data);
	if (this->data.size() == 0) {
		stats.has_stats = true;
		stats.min = min;
		stats.max = max;
	} else {
		Value::Min(min, stats.min, stats.min);
		Value::Max(max, stats.max, stats.max);

		// check if this vector fits into the last vector of the column
		// if it does, just append it
		auto &back = this->data.back();
		if (back->count + data.count < back->maximum_size) {
			back->Append(data);
			return;
		}
	}
	this->data.push_back(make_unique<Vector>());
	// base tables need to own the data
	// if <data> owns the data we can take it
	// otherwise we need to make a copy
	data.Move(*this->data.back());
	this->data.back()->ForceOwnership();
}
