//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/parquet_write_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/parquet_write_stats.hpp"
#include "zstd/common/xxhash.hpp"
#include "duckdb/common/types/uhugeint.hpp"

namespace duckdb {

struct BaseParquetOperator {
	template <class SRC, class TGT>
	static void WriteToStream(const TGT &input, WriteStream &ser) {
		ser.WriteData(const_data_ptr_cast(&input), sizeof(TGT));
	}

	template <class SRC, class TGT>
	static constexpr idx_t WriteSize(const TGT &input) {
		return sizeof(TGT);
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(&target_value, sizeof(target_value), 0);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return nullptr;
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}

	template <class SRC, class TGT>
	static idx_t GetRowSize(const Vector &, idx_t) {
		return sizeof(TGT);
	}
};

struct ParquetCastOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<NumericStatisticsState<SRC, TGT, BaseParquetOperator>>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
		auto &numeric_stats = (NumericStatisticsState<SRC, TGT, BaseParquetOperator> &)*stats;
		if (LessThan::Operation(target_value, numeric_stats.min)) {
			numeric_stats.min = target_value;
		}
		if (GreaterThan::Operation(target_value, numeric_stats.max)) {
			numeric_stats.max = target_value;
		}
	}
};

struct ParquetTimestampNSOperator : public ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
};

struct ParquetTimestampSOperator : public ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochSecondsPossiblyInfinite(input).value;
	}
};

struct ParquetStringOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return input;
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<StringStatisticsState>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
		auto &string_stats = stats->Cast<StringStatisticsState>();
		string_stats.Update(target_value);
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.Write<uint32_t>(target_value.GetSize());
		ser.WriteData(const_data_ptr_cast(target_value.GetData()), target_value.GetSize());
	}

	template <class SRC, class TGT>
	static idx_t WriteSize(const TGT &target_value) {
		return sizeof(uint32_t) + target_value.GetSize();
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.GetData(), target_value.GetSize(), 0);
	}

	template <class SRC, class TGT>
	static idx_t GetRowSize(const Vector &vector, idx_t index) {
		return FlatVector::GetData<string_t>(vector)[index].GetSize();
	}
};

struct ParquetIntervalTargetType {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;
	data_t bytes[PARQUET_INTERVAL_SIZE];
};

struct ParquetIntervalOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		if (input.days < 0 || input.months < 0 || input.micros < 0) {
			throw IOException("Parquet files do not support negative intervals");
		}
		TGT result;
		Store<uint32_t>(input.months, result.bytes);
		Store<uint32_t>(input.days, result.bytes + sizeof(uint32_t));
		Store<uint32_t>(input.micros / 1000, result.bytes + sizeof(uint32_t) * 2);
		return result;
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.WriteData(target_value.bytes, ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE);
	}

	template <class SRC, class TGT>
	static constexpr idx_t WriteSize(const TGT &target_value) {
		return ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE;
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.bytes, ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE, 0);
	}
};

struct ParquetUUIDTargetType {
	static constexpr const idx_t PARQUET_UUID_SIZE = 16;
	data_t bytes[PARQUET_UUID_SIZE];
};

struct ParquetUUIDOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		TGT result;
		uint64_t high_bytes = input.upper ^ (int64_t(1) << 63);
		uint64_t low_bytes = input.lower;
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
			result.bytes[i] = (high_bytes >> shift_count) & 0xFF;
		}
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
			result.bytes[sizeof(uint64_t) + i] = (low_bytes >> shift_count) & 0xFF;
		}
		return result;
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.WriteData(target_value.bytes, ParquetUUIDTargetType::PARQUET_UUID_SIZE);
	}

	template <class SRC, class TGT>
	static constexpr idx_t WriteSize(const TGT &target_value) {
		return ParquetUUIDTargetType::PARQUET_UUID_SIZE;
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.bytes, ParquetUUIDTargetType::PARQUET_UUID_SIZE, 0);
	}
};

struct ParquetTimeTZOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return input.time().micros;
	}
};

struct ParquetHugeintOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Hugeint::Cast<double>(input);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<ColumnWriterStatistics>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}
};

struct ParquetUhugeintOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Uhugeint::Cast<double>(input);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<ColumnWriterStatistics>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}
};

} // namespace duckdb
