#include "dsdgen.hpp"

#include "append_info-c.hpp"
#include "dsdgen_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "tpcds_constants.hpp"
#include "dsdgen_schema.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

#include <cassert>

using namespace duckdb;
using namespace std;

namespace tpcds {

template <class T>
static void CreateTPCDSTable(ClientContext &context, string catalog_name, string schema, string suffix, bool keys,
                             bool overwrite) {
	auto info = make_uniq<CreateTableInfo>();
	info->schema = schema;
	info->table = T::Name + suffix;
	info->on_conflict = overwrite ? OnCreateConflict::REPLACE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->temporary = false;
	for (idx_t i = 0; i < T::ColumnCount; i++) {
		info->columns.AddColumn(ColumnDefinition(T::Columns[i], T::Types[i]));
	}
	if (keys) {
		duckdb::vector<string> pk_columns;
		for (idx_t i = 0; i < T::PrimaryKeyCount; i++) {
			pk_columns.push_back(T::PrimaryKeyColumns[i]);
		}
		info->constraints.push_back(make_uniq<UniqueConstraint>(std::move(pk_columns), true));
	}
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	catalog.CreateTable(context, std::move(info));
}

void DSDGenWrapper::CreateTPCDSSchema(ClientContext &context, string catalog, string schema, string suffix, bool keys,
                                      bool overwrite) {
	CreateTPCDSTable<CallCenterInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CatalogPageInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CatalogReturnsInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CatalogSalesInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CustomerInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CustomerAddressInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<CustomerDemographicsInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<DateDimInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<HouseholdDemographicsInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<IncomeBandInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<InventoryInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<ItemInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<PromotionInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<ReasonInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<ShipModeInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<StoreInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<StoreReturnsInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<StoreSalesInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<TimeDimInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<WarehouseInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<WebPageInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<WebReturnsInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<WebSalesInfo>(context, catalog, schema, suffix, keys, overwrite);
	CreateTPCDSTable<WebSiteInfo>(context, catalog, schema, suffix, keys, overwrite);
}

void DSDGenWrapper::DSDGen(double scale, ClientContext &context, string catalog_name, string schema, string suffix) {
	if (scale <= 0) {
		// schema only
		return;
	}

	InitializeDSDgen(scale);

	// populate append info
	duckdb::vector<duckdb::unique_ptr<tpcds_append_information>> append_info;
	append_info.resize(DBGEN_VERSION);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);

	int tmin = CALL_CENTER, tmax = DBGEN_VERSION;

	for (int table_id = tmin; table_id < tmax; table_id++) {
		auto table_def = GetTDefByNumber(table_id);
		auto table_name = table_def.name + suffix;
		assert(table_def.name);
		auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);

		auto append = make_uniq<tpcds_append_information>(context, &table_entry);
		append->table_def = table_def;
		append_info[table_id] = std::move(append);
	}

	// actually generate tables using modified data generator functions
	for (int table_id = tmin; table_id < tmax; table_id++) {
		// child tables are created in parent loaders
		if (append_info[table_id]->table_def.fl_child) {
			continue;
		}

		ds_key_t k_row_count = GetRowCount(table_id), k_first_row = 1;

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (append_info[table_id]->table_def.fl_small) {
			ResetCountCount();
		}

		auto builder_func = GetTDefFunctionByNumber(table_id);
		assert(builder_func);

		for (ds_key_t i = k_first_row; k_row_count; i++, k_row_count--) {
			// append happens directly in builders since they dump child tables
			// immediately
			if (builder_func((void *)&append_info, i)) {
				throw Exception("Table generation failed");
			}
		}
	}

	// flush any incomplete chunks
	for (int table_id = tmin; table_id < tmax; table_id++) {
		append_info[table_id]->appender.Close();
	}
}

uint32_t DSDGenWrapper::QueriesCount() {
	return TPCDS_QUERIES_COUNT;
}

string DSDGenWrapper::GetQuery(int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	return TPCDS_QUERIES[query - 1];
}

string DSDGenWrapper::GetAnswer(double sf, int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}

	if (sf == 0.01) {
		return TPCDS_ANSWERS_SF0_01[query - 1];
	} else if (sf == 1) {
		return TPCDS_ANSWERS_SF1[query - 1];
	} else {
		throw NotImplementedException("Don't have TPC-DS answers for SF %llf!", sf);
	}
}

} // namespace tpcds
