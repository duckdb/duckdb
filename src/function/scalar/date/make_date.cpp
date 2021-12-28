#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

namespace duckdb {

struct HexaryExecutor {
public:
	template <class TA, class TB, class TC, class TD, class TE, class TF, class TR,
	          class FUN = std::function<TR(TA, TB, TC, TD, TE, TF)>>
	static void Execute(DataChunk &input, Vector &result, FUN fun) {
		D_ASSERT(input.ColumnCount() == 6);
		const auto count = input.size();

		bool all_constant = true;
		bool any_null = false;
		for (const auto &v : input.data) {
			if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				if (ConstantVector::IsNull(v)) {
					any_null = true;
				}
			} else {
				all_constant = false;
				break;
			}
		}

		if (all_constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			if (any_null) {
				ConstantVector::SetNull(result, true);
			} else {
				auto adata = ConstantVector::GetData<TA>(input.data[0]);
				auto bdata = ConstantVector::GetData<TB>(input.data[1]);
				auto cdata = ConstantVector::GetData<TC>(input.data[2]);
				auto ddata = ConstantVector::GetData<TD>(input.data[3]);
				auto edata = ConstantVector::GetData<TE>(input.data[4]);
				auto fdata = ConstantVector::GetData<TF>(input.data[5]);
				auto result_data = ConstantVector::GetData<TR>(result);
				result_data[0] = fun(*adata, *bdata, *cdata, *ddata, *edata, *fdata);
			}
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<TR>(result);
			auto &result_validity = FlatVector::Validity(result);

			auto adata = FlatVector::GetData<TA>(input.data[0]);
			auto bdata = FlatVector::GetData<TB>(input.data[1]);
			auto cdata = FlatVector::GetData<TC>(input.data[2]);
			auto ddata = FlatVector::GetData<TD>(input.data[3]);
			auto edata = FlatVector::GetData<TE>(input.data[4]);
			auto fdata = FlatVector::GetData<TF>(input.data[5]);

			bool all_valid = true;
			vector<VectorData> vdata(input.ColumnCount());
			for (size_t c = 0; c < vdata.size(); ++c) {
				input.data[c].Orrify(count, vdata[c]);
				all_valid = all_valid && vdata[c].validity.AllValid();
			}

			vector<idx_t> idx(vdata.size());
			if (all_valid) {
				for (idx_t r = 0; r < count; ++r) {
					for (idx_t c = 0; c < idx.size(); ++c) {
						idx[c] = vdata[c].sel->get_index(r);
					}
					result_data[r] =
					    fun(adata[idx[0]], bdata[idx[1]], cdata[idx[2]], ddata[idx[3]], edata[idx[4]], fdata[idx[5]]);
				}
			} else {
				all_valid = true;
				for (idx_t r = 0; r < count; ++r) {
					for (idx_t c = 0; c < idx.size(); ++c) {
						if (!vdata[c].validity.RowIsValid(r)) {
							all_valid = false;
							result_validity.SetInvalid(r);
							break;
						}
					}
					if (all_valid) {
						for (idx_t c = 0; c < idx.size(); ++c) {
							idx[c] = vdata[c].sel->get_index(r);
						}
						result_data[r] = fun(adata[idx[0]], bdata[idx[1]], cdata[idx[2]], ddata[idx[3]], edata[idx[4]],
						                     fdata[idx[5]]);
					}
				}
			}
		}
	}
};

struct MakeDateOperator {
	template <typename YYYY, typename MM, typename DD, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd) {
		return Date::FromDate(yyyy, mm, dd);
	}
};

template <typename T>
static void ExecuteMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(),
	                                          MakeDateOperator::Operation<T, T, T, date_t>);
}

struct MakeTimeOperator {
	template <typename HH, typename MM, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(HH hh, MM mm, SS ss) {
		int64_t secs = ss;
		int64_t micros = std::round((ss - secs) * Interval::MICROS_PER_SEC);
		return Time::FromTime(hh, mm, secs, micros);
	}
};

template <typename T>
static void ExecuteMakeTime(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, double, dtime_t>(yyyy, mm, dd, result, input.size(),
	                                                MakeTimeOperator::Operation<T, T, double, dtime_t>);
}

struct MakeTimestampOperator {
	template <typename YYYY, typename MM, typename DD, typename HR, typename MN, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd, HR hr, MN mn, SS ss) {
		const auto d = MakeDateOperator::Operation<YYYY, MM, DD, date_t>(yyyy, mm, dd);
		const auto t = MakeTimeOperator::Operation<HR, MN, SS, dtime_t>(hr, mn, ss);
		return Timestamp::FromDatetime(d, t);
	}
};

template <typename T>
static void ExecuteMakeTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 6);

	auto func = MakeTimestampOperator::Operation<T, T, T, T, T, double, timestamp_t>;
	HexaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(input, result, func);
}

void MakeDateFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet make_date("make_date");
	make_date.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::DATE, ExecuteMakeDate<int64_t>));
	set.AddFunction(make_date);

	ScalarFunctionSet make_time("make_time");
	make_time.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                     LogicalType::TIME, ExecuteMakeTime<int64_t>));
	set.AddFunction(make_time);

	ScalarFunctionSet make_timestamp("make_timestamp");
	make_timestamp.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                           LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                          LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	set.AddFunction(make_timestamp);
}

} // namespace duckdb
