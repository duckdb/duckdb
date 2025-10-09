#include "duckdb/planner/operator/logical_vacuum.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

LogicalVacuum::LogicalVacuum() : LogicalOperator(LogicalOperatorType::LOGICAL_VACUUM) {
}

LogicalVacuum::LogicalVacuum(unique_ptr<VacuumInfo> info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_VACUUM), info(std::move(info)) {
}

TableCatalogEntry &LogicalVacuum::GetTable() {
	D_ASSERT(HasTable());
	return *table;
}
bool LogicalVacuum::HasTable() const {
	return table != nullptr;
}

void LogicalVacuum::SetTable(TableCatalogEntry &table_p) {
	table = &table_p;
}

void LogicalVacuum::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);

	serializer.WriteProperty(200, "info", info);
	serializer.WriteProperty(201, "column_id_map", column_id_map);
}

unique_ptr<LogicalOperator> LogicalVacuum::Deserialize(Deserializer &deserializer) {
	auto result = unique_ptr<LogicalVacuum>(new LogicalVacuum());

	auto tmp_info = deserializer.ReadPropertyWithDefault<unique_ptr<ParseInfo>>(200, "info");
	deserializer.ReadProperty(201, "column_id_map", result->column_id_map);

	result->info = unique_ptr_cast<ParseInfo, VacuumInfo>(std::move(tmp_info));
	auto &info = *result->info;
	if (info.has_table) {
		// deserialize the 'table'
		auto &context = deserializer.Get<ClientContext &>();
		auto binder = Binder::CreateBinder(context);
		auto bound_table = binder->Bind(*info.ref);
		if (bound_table.plan->type != LogicalOperatorType::LOGICAL_GET) {
			throw InvalidInputException("can only vacuum or analyze base tables");
		}
		auto table_ptr = bound_table.plan->Cast<LogicalGet>().GetTable();
		if (!table_ptr) {
			throw InvalidInputException("can only vacuum or analyze base tables");
		}
		auto &table = *table_ptr;
		result->SetTable(table);
		// FIXME: we should probably verify that the 'column_id_map' and 'columns' are the same on the bound table after
		// deserialization?
	}
	return std::move(result);
}

idx_t LogicalVacuum::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
