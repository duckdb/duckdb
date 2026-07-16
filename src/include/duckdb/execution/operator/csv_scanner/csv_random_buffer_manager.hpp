//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_random_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"

namespace duckdb {

//! Buffer manager for files with known buffer byte ranges: any buffer can be materialized independently
//! through a positional read, in any order
class CSVRandomBufferManager : public CSVBufferManager {
public:
	CSVRandomBufferManager(ClientContext &context, const CSVReaderOptions &options, const OpenFileInfo &file,
	                       bool per_file_single_threaded, unique_ptr<CSVFileHandle> file_handle);

	shared_ptr<CSVBufferHandle> GetBuffer(const idx_t buffer_idx) override;
	CSVBufferResidency GetBufferResidency(const idx_t buffer_idx, shared_ptr<CSVBufferHandle> &handle) override;
	void ResetBuffer(const idx_t buffer_idx) override;
	bool Done() const override;
	void ResetBufferManager() override;

private:
	//! Sizes the buffer table and eagerly materializes the first buffer (sniffing and newline detection
	//! always read it)
	void Initialize();
};

} // namespace duckdb
