#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstring>
#include <string>

using namespace duckdb;

// Regression test for duckdb-python#475 / duckdb-spatial#788.
//
// Converting a GEOMETRY column that carries a CRS to an Arrow schema invokes the
// internal geoarrow.wkb arrow extension: ArrowGeometry::PopulateSchema ->
// WriteCRS -> CoordinateReferenceSystem::TryConvert, which does a catalog lookup
// (Catalog::GetEntry -> GetCatalogTransaction) and therefore requires an active
// transaction. When an Arrow result is consumed *after* the producing
// (auto-commit) transaction has closed, rebuilding the schema asserts with
// "TransactionContext::ActiveTransaction called without active transaction".
//
// The fix: PhysicalArrowCollector / PhysicalArrowBatchCollector build the schema
// during Finalize -- while the producing transaction is still active -- and
// cache it on the ArrowQueryResult, so consumers never rebuild it post-commit.

// Scan an Arrow C Data Interface metadata blob for a key or value substring.
static bool MetadataContains(const char *metadata, const std::string &needle) {
	if (!metadata) {
		return false;
	}
	int32_t n_entries;
	std::memcpy(&n_entries, metadata, sizeof(int32_t));
	const char *ptr = metadata + sizeof(int32_t);
	for (int32_t i = 0; i < n_entries; i++) {
		int32_t key_len;
		std::memcpy(&key_len, ptr, sizeof(int32_t));
		ptr += sizeof(int32_t);
		std::string key(ptr, static_cast<size_t>(key_len));
		ptr += key_len;
		int32_t val_len;
		std::memcpy(&val_len, ptr, sizeof(int32_t));
		ptr += sizeof(int32_t);
		std::string val(ptr, static_cast<size_t>(val_len));
		ptr += val_len;
		if (key.find(needle) != std::string::npos || val.find(needle) != std::string::npos) {
			return true;
		}
	}
	return false;
}

static void ForceArrowCollector(Connection &con) {
	auto &config = ClientConfig::GetConfig(*con.context);
	config.get_result_collector = [](ClientContext &context, PreparedStatementData &data) {
		return PhysicalArrowCollector::Create(context, data, STANDARD_VECTOR_SIZE);
	};
}

TEST_CASE("Arrow schema for GEOMETRY with CRS survives transaction commit (#475)", "[arrow]") {
	DuckDB db;
	Connection con(db);
	ForceArrowCollector(con);

	// 'OGC:CRS84' is an authority-code CRS, so serializing it forces the catalog
	// lookup in CoordinateReferenceSystem::TryConvert.
	// Use ClientContext::Query (not Connection::Query, which materializes) so the
	// get_result_collector override produces an ArrowQueryResult.
	auto result = con.context->Query("SELECT 'POINT(0 1)'::GEOMETRY('OGC:CRS84') AS g", false);
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->type == QueryResultType::ARROW_RESULT);
	auto &arrow = result->Cast<ArrowQueryResult>();

	// The producing transaction has auto-committed. Rebuilding the schema now --
	// the way consumers used to before the fix -- reaches the CRS catalog lookup
	// with no active transaction and throws. This is the #475 failure mode.
	SECTION("rebuilding the schema post-commit reproduces the failure") {
		REQUIRE_THROWS([&]() {
			ArrowSchema rebuilt;
			rebuilt.release = nullptr;
			ArrowConverter::ToArrowSchema(&rebuilt, arrow.types, arrow.names, arrow.client_properties);
			if (rebuilt.release) {
				rebuilt.release(&rebuilt);
			}
		}());
	}

	// The fix: the collector cached the schema while the producing transaction
	// was still active. GetSchema() hands consumers an independent, owned copy
	// without a transaction -- this is the accessor consumers must use instead of
	// rebuilding via ToArrowSchema. The copy carries the geoarrow.wkb extension
	// metadata that only the in-transaction build could produce.
	SECTION("the collector caches the schema built under the producing transaction") {
		REQUIRE(arrow.HasCachedSchema());
		ArrowSchema copied;
		copied.release = nullptr;
		arrow.GetSchema(copied);
		REQUIRE(copied.release != nullptr);
		REQUIRE(copied.n_children == 1);
		REQUIRE(copied.children[0] != nullptr);
		REQUIRE(MetadataContains(copied.children[0]->metadata, "geoarrow.wkb"));
		copied.release(&copied);
	}
}
