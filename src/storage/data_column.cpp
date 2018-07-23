
#include "common/helper.hpp"

#include "storage/data_column.hpp"
#include "storage/storage_manager.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void DataColumn::AddData(Vector& data) {
	if (data.type != column.type) {
		throw CatalogException("Mismatch in column type");
	}
	this->data.push_back(make_unique<Vector>());
	data.Move(*this->data.back());
}

