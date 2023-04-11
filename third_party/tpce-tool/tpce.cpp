#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/helper.hpp"

#include "tpce_generated.hpp"
#include "tpce.hpp"

#include "main/EGenLoader_stdafx.h"
#include "input/DataFileManager.h"

using namespace duckdb;
using namespace TPCE;
using namespace std;

namespace tpce {

TIdent iStartFromCustomer = iDefaultStartFromCustomer;
TIdent iCustomerCount = iDefaultCustomerCount;      // # of customers for this instance
TIdent iTotalCustomerCount = iDefaultCustomerCount; // total number of customers in the database
UINT iLoadUnitSize = iDefaultLoadUnitSize;          // # of customers in one load unit
UINT iDaysOfInitialTrades = 300;

void dbgen(duckdb::DuckDB &db, uint32_t sf, std::string schema, std::string suffix) {
	duckdb::unique_ptr<CBaseLoaderFactory> pLoaderFactory; // class factory that creates table loaders
	CGenerateAndLoadStandardOutput Output;
	duckdb::unique_ptr<CGenerateAndLoad> pGenerateAndLoad;

	Connection con(db);
	con.Query("BEGIN TRANSACTION");

	CreateTPCESchema(db, con, schema, suffix);

	if (sf == 0) {
		// schema only
		con.Query("COMMIT");
		return;
	}

	pLoaderFactory = make_uniq<DuckDBLoaderFactory>(con, schema, suffix);

	// Create log formatter and logger instance
	CLogFormatTab fmt;
	CEGenLogger logger(eDriverEGenLoader, 0, nullptr, &fmt);

	// Set up data file manager for lazy load.
	const DataFileManager dfm(iTotalCustomerCount, iTotalCustomerCount);

	// Create the main class instance
	pGenerateAndLoad = make_uniq<CGenerateAndLoad>(dfm, iCustomerCount, iStartFromCustomer, iTotalCustomerCount, iLoadUnitSize, sf,
	                         iDaysOfInitialTrades, pLoaderFactory.get(), &logger, &Output, true);

	//  The generate and load phase starts here.
	// Generate static tables
	pGenerateAndLoad->GenerateAndLoadFixedTables();

	// Generate dynamic scaling tables
	pGenerateAndLoad->GenerateAndLoadScalingTables();

	// Generate dynamic trade tables
	pGenerateAndLoad->GenerateAndLoadGrowingTables();

	con.Query("COMMIT");
}

} // namespace tpce
