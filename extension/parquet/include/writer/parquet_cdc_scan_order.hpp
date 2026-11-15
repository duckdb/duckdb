//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/parquet_cdc_scan_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_idx.hpp"

#include <condition_variable>
#include <exception>
#include <mutex>

namespace duckdb {

class ParquetCdcScanOrder;

//! The right to take content-defined page cuts, held by one batch at a time. Released, and the next batch
//! woken, when it goes out of scope. If the guarded work throws, the scan is marked broken on the way out so
//! the batches behind it report the failure instead of waiting for a turn that will never come.
class ParquetCdcScanTurn {
public:
	explicit ParquetCdcScanTurn(ParquetCdcScanOrder &order_p) : order(order_p), uncaught(std::uncaught_exceptions()) {
	}
	~ParquetCdcScanTurn();

	ParquetCdcScanTurn(const ParquetCdcScanTurn &) = delete;

private:
	ParquetCdcScanOrder &order;
	int uncaught;
};

//! Content-defined page cuts depend on the values that came before them, so they have to be taken in the order
//! the file sees its rows in, even though the batches they come from are prepared in parallel. This hands the
//! scan out to one batch at a time, in batch index order; the heavy work that follows (statistics, dictionary,
//! compression) is not covered by the turn and stays concurrent. See ParquetWriter::ScanRowGroup.
class ParquetCdcScanOrder {
public:
	//! Waits until it is this batch's turn. Throws what an earlier batch failed with, if one did.
	ParquetCdcScanTurn Acquire(optional_idx batch_index) {
		D_ASSERT(batch_index.IsValid());
		auto index = batch_index.GetIndex();
		if (index < next_batch_index) {
			throw InternalException("Content-defined page cuts were requested twice for batch %llu", index);
		}
		std::unique_lock<std::mutex> guard(lock);
		cv.wait(guard, [&]() { return index == next_batch_index || broken; });
		if (broken) {
			ThrowFailure();
		}
		return ParquetCdcScanTurn(*this);
	}

	//! Records why the scan stopped, for whoever is waiting to be rethrown at
	void Fail(const std::exception_ptr &error_p) {
		std::lock_guard<std::mutex> guard(lock);
		if (!error) {
			error = error_p;
		}
	}

private:
	friend class ParquetCdcScanTurn;

	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		next_batch_index++;
		cv.notify_all();
	}

	[[noreturn]] void ThrowFailure() const {
		if (error) {
			std::rethrow_exception(error);
		}
		throw IOException("Content-defined page cuts were abandoned after an earlier batch failed");
	}

	mutable std::mutex lock;
	std::condition_variable cv;
	idx_t next_batch_index = 0;
	bool broken = false;
	std::exception_ptr error;
};

inline ParquetCdcScanTurn::~ParquetCdcScanTurn() {
	if (std::uncaught_exceptions() > uncaught) {
		order.Fail(std::current_exception());
	}
	order.Release();
}

} // namespace duckdb
