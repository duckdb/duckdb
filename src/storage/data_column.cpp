
#include "common/helper.hpp"

#include "storage/data_column.hpp"
#include "storage/storage_manager.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void DataColumn::AddData(Vector &data) {
	if (data.type != column.type) {
		throw CatalogException("Mismatch in column type");
	}
	if (this->data.size() > 0) {
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
