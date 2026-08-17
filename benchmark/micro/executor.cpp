#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/variadic_executor.hpp"

using namespace duckdb;

namespace {

enum class ExecutorBenchmarkType : uint8_t {
	UNARY_EXECUTE,
	BINARY_EXECUTE,
	TERNARY_EXECUTE,
	TERNARY_OPTIONAL_EXECUTE,
	VARIADIC_EXECUTE,
	BINARY_SELECT,
	BINARY_SELECT_GREATER_THAN_EQUALS,
	TERNARY_SELECT
};

enum class ExecutorNullProfile : uint8_t { NONE, SPARSE, DENSE, MOSTLY_NULL, CLUSTERED };

enum class ExecutorSelectionOutput : uint8_t { BOTH, TRUE_ONLY, FALSE_ONLY };

static constexpr idx_t EXECUTOR_BENCHMARK_COUNT = STANDARD_VECTOR_SIZE;
static constexpr idx_t EXECUTOR_BENCHMARK_ITERATIONS = 1000000;
static constexpr idx_t EXECUTOR_CONSTANT_BENCHMARK_ITERATIONS = 20000000;

struct ExecutorBetweenOperator {
	template <class T>
	static bool Operation(T input, T lower, T upper) {
		return input >= lower && input <= upper;
	}
};

struct ExecutorBenchmarkState : public DuckDBBenchmarkState {
	ExecutorBenchmarkState(uint8_t constant_mask_p, uint8_t dictionary_mask_p, ExecutorNullProfile null_profile_p,
	                       idx_t selection_percent_p)
	    : DuckDBBenchmarkState(string()), a(LogicalType::BIGINT, EXECUTOR_BENCHMARK_COUNT),
	      b(LogicalType::BIGINT, EXECUTOR_BENCHMARK_COUNT), c(LogicalType::BIGINT, EXECUTOR_BENCHMARK_COUNT),
	      d(LogicalType::BIGINT, EXECUTOR_BENCHMARK_COUNT), result(LogicalType::BIGINT, EXECUTOR_BENCHMARK_COUNT),
	      true_sel(EXECUTOR_BENCHMARK_COUNT), false_sel(EXECUTOR_BENCHMARK_COUNT), constant_mask(constant_mask_p),
	      dictionary_mask(dictionary_mask_p), null_profile(null_profile_p), selection_percent(selection_percent_p) {
		InitializeInput(a, 1, 0);
		InitializeInput(b, 3, 1);
		InitializeInput(c, 5, 2);
		InitializeInput(d, 7, 3);
		if (selection_percent <= 100 && constant_mask == 0x5) {
			auto b_data = FlatVector::GetDataMutable<int64_t>(b);
			for (idx_t i = 0; i < EXECUTOR_BENCHMARK_COUNT; i++) {
				b_data[i] = i % 100 < selection_percent ? 1 : 3;
			}
		} else if (selection_percent <= 100 && !(constant_mask & 0x3)) {
			auto a_data = FlatVector::GetData<int64_t>(a);
			auto b_data = FlatVector::GetDataMutable<int64_t>(b);
			for (idx_t i = 0; i < EXECUTOR_BENCHMARK_COUNT; i++) {
				b_data[i] = a_data[i] + (i % 100 < selection_percent ? 1 : -1);
			}
		}
	}

	void InitializeInput(Vector &input, int64_t multiplier, idx_t input_index) {
		auto data = FlatVector::GetDataMutable<int64_t>(input);
		for (idx_t i = 0; i < EXECUTOR_BENCHMARK_COUNT; i++) {
			data[i] = NumericCast<int64_t>(i * multiplier + input_index);
		}
		FlatVector::SetSize(input, EXECUTOR_BENCHMARK_COUNT);
		ApplyNullProfile(input, input_index);
		if (constant_mask & (uint8_t(1) << input_index)) {
			input.Reference(Value::BIGINT(data[0]), count_t(EXECUTOR_BENCHMARK_COUNT));
		} else if (dictionary_mask & (uint8_t(1) << input_index)) {
			SelectionVector sel(EXECUTOR_BENCHMARK_COUNT);
			for (idx_t i = 0; i < EXECUTOR_BENCHMARK_COUNT; i++) {
				sel.set_index(i, i % 128);
			}
			input.Slice(sel, EXECUTOR_BENCHMARK_COUNT);
		}
	}

	void ApplyNullProfile(Vector &input, idx_t input_index) {
		if (null_profile == ExecutorNullProfile::NONE) {
			return;
		}
		auto &validity = FlatVector::ValidityMutable(input);
		for (idx_t row = 0; row < EXECUTOR_BENCHMARK_COUNT; row++) {
			auto shifted_row = row + input_index * 17;
			bool is_null = false;
			switch (null_profile) {
			case ExecutorNullProfile::NONE:
				break;
			case ExecutorNullProfile::SPARSE:
				is_null = shifted_row % 100 == 0;
				break;
			case ExecutorNullProfile::DENSE:
				is_null = shifted_row % 2 == 0;
				break;
			case ExecutorNullProfile::MOSTLY_NULL:
				is_null = shifted_row % 100 != 0;
				break;
			case ExecutorNullProfile::CLUSTERED:
				is_null = (shifted_row / ValidityMask::BITS_PER_VALUE) % 2 == 0;
				break;
			}
			if (is_null) {
				validity.SetInvalid(row);
			}
		}
	}

	Vector a;
	Vector b;
	Vector c;
	Vector d;
	Vector result;
	SelectionVector true_sel;
	SelectionVector false_sel;
	uint8_t constant_mask;
	uint8_t dictionary_mask;
	ExecutorNullProfile null_profile;
	idx_t selection_percent;
	idx_t selected_count = 0;
	int64_t checksum = 0;
};

class ExecutorBenchmark : public DuckDBBenchmark {
public:
	ExecutorBenchmark(const string &name, ExecutorBenchmarkType type_p, uint8_t constant_mask_p = 0,
	                  uint8_t dictionary_mask_p = 0, ExecutorNullProfile null_profile_p = ExecutorNullProfile::NONE,
	                  idx_t selection_percent_p = 101,
	                  ExecutorSelectionOutput selection_output_p = ExecutorSelectionOutput::BOTH)
	    : DuckDBBenchmark(true, name, "[scalar_executor]"), type(type_p), constant_mask(constant_mask_p),
	      dictionary_mask(dictionary_mask_p), null_profile(null_profile_p), selection_percent(selection_percent_p),
	      selection_output(selection_output_p) {
	}

	unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {
		return make_uniq<ExecutorBenchmarkState>(constant_mask, dictionary_mask, null_profile, selection_percent);
	}

	void Load(DuckDBBenchmarkState *state) override {
	}

	void RunBenchmark(DuckDBBenchmarkState *state_p) override {
		auto &state = static_cast<ExecutorBenchmarkState &>(*state_p);
		const auto iterations = Iterations();
		switch (type) {
		case ExecutorBenchmarkType::UNARY_EXECUTE: {
			auto fun = [](int64_t value) {
				return value + 1;
			};
			for (idx_t i = 0; i < iterations; i++) {
				UnaryExecutor::Execute<int64_t, int64_t>(state.a, state.result, fun, FunctionErrors::CANNOT_ERROR);
			}
			break;
		}
		case ExecutorBenchmarkType::BINARY_EXECUTE: {
			auto fun = [](int64_t left, int64_t right) {
				return left + right;
			};
			for (idx_t i = 0; i < iterations; i++) {
				BinaryExecutor::Execute<int64_t, int64_t, int64_t>(state.a, state.b, state.result, fun);
			}
			break;
		}
		case ExecutorBenchmarkType::TERNARY_EXECUTE: {
			auto fun = [](int64_t a, int64_t b, int64_t c) {
				return a + b * c;
			};
			for (idx_t i = 0; i < iterations; i++) {
				TernaryExecutor::Execute<int64_t, int64_t, int64_t, int64_t>(state.a, state.b, state.c, state.result,
				                                                             fun);
			}
			break;
		}
		case ExecutorBenchmarkType::TERNARY_OPTIONAL_EXECUTE: {
			auto fun = [](int64_t a, int64_t b, int64_t c) -> optional<int64_t> {
				auto value = a + b * c;
				return value % 101 == 0 ? optional<int64_t>() : optional<int64_t>(value);
			};
			for (idx_t i = 0; i < iterations; i++) {
				TernaryExecutor::Execute<int64_t, int64_t, int64_t, int64_t>(state.a, state.b, state.c, state.result,
				                                                             fun);
			}
			break;
		}
		case ExecutorBenchmarkType::VARIADIC_EXECUTE: {
			auto fun = [](int64_t a, int64_t b, int64_t c, int64_t d) {
				return a + b * c - d;
			};
			std::array<VariadicExecutor::VectorRef, 4> inputs = {{state.a, state.b, state.c, state.d}};
			for (idx_t i = 0; i < iterations; i++) {
				VariadicExecutor::Execute<int64_t, int64_t, int64_t, int64_t, int64_t>(inputs, state.result, fun);
			}
			break;
		}
		case ExecutorBenchmarkType::BINARY_SELECT: {
			auto true_sel = selection_output == ExecutorSelectionOutput::FALSE_ONLY ? nullptr : &state.true_sel;
			auto false_sel = selection_output == ExecutorSelectionOutput::TRUE_ONLY ? nullptr : &state.false_sel;
			for (idx_t i = 0; i < iterations; i++) {
				state.selected_count = BinaryExecutor::Select<int64_t, int64_t, LessThan>(
				    state.a, state.b, nullptr, EXECUTOR_BENCHMARK_COUNT, true_sel, false_sel);
			}
			break;
		}
		case ExecutorBenchmarkType::BINARY_SELECT_GREATER_THAN_EQUALS: {
			auto true_sel = selection_output == ExecutorSelectionOutput::FALSE_ONLY ? nullptr : &state.true_sel;
			auto false_sel = selection_output == ExecutorSelectionOutput::TRUE_ONLY ? nullptr : &state.false_sel;
			for (idx_t i = 0; i < iterations; i++) {
				state.selected_count = BinaryExecutor::Select<int64_t, int64_t, GreaterThanEquals>(
				    state.a, state.b, nullptr, EXECUTOR_BENCHMARK_COUNT, true_sel, false_sel);
			}
			break;
		}
		case ExecutorBenchmarkType::TERNARY_SELECT: {
			auto true_sel = selection_output == ExecutorSelectionOutput::FALSE_ONLY ? nullptr : &state.true_sel;
			auto false_sel = selection_output == ExecutorSelectionOutput::TRUE_ONLY ? nullptr : &state.false_sel;
			for (idx_t i = 0; i < iterations; i++) {
				state.selected_count = TernaryExecutor::Select<int64_t, int64_t, int64_t, ExecutorBetweenOperator>(
				    state.b, state.a, state.c, nullptr, EXECUTOR_BENCHMARK_COUNT, true_sel, false_sel);
			}
			break;
		}
		}
		if (type == ExecutorBenchmarkType::BINARY_SELECT ||
		    type == ExecutorBenchmarkType::BINARY_SELECT_GREATER_THAN_EQUALS ||
		    type == ExecutorBenchmarkType::TERNARY_SELECT) {
			state.checksum = NumericCast<int64_t>(state.selected_count + 1);
		} else {
			auto value = state.result.GetValue(EXECUTOR_BENCHMARK_COUNT - 1);
			state.checksum = value.IsNull() ? 1 : value.GetValue<int64_t>();
		}
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}

	string Verify(BenchmarkState *state_p) override {
		auto &state = static_cast<ExecutorBenchmarkState &>(*state_p);
		return state.checksum == 0 ? "Executor benchmark produced an empty checksum" : string();
	}

	string BenchmarkInfo() override {
		return "Direct scalar executor microbenchmark over a standard-size vector";
	}

private:
	idx_t Iterations() const {
		switch (type) {
		case ExecutorBenchmarkType::UNARY_EXECUTE:
			return constant_mask == 0x1 ? EXECUTOR_CONSTANT_BENCHMARK_ITERATIONS : EXECUTOR_BENCHMARK_ITERATIONS;
		case ExecutorBenchmarkType::BINARY_EXECUTE:
		case ExecutorBenchmarkType::BINARY_SELECT:
		case ExecutorBenchmarkType::BINARY_SELECT_GREATER_THAN_EQUALS:
		case ExecutorBenchmarkType::TERNARY_SELECT:
			return constant_mask == 0x3 ? EXECUTOR_CONSTANT_BENCHMARK_ITERATIONS : EXECUTOR_BENCHMARK_ITERATIONS;
		case ExecutorBenchmarkType::TERNARY_EXECUTE:
		case ExecutorBenchmarkType::TERNARY_OPTIONAL_EXECUTE:
			return constant_mask == 0x7 ? EXECUTOR_CONSTANT_BENCHMARK_ITERATIONS : EXECUTOR_BENCHMARK_ITERATIONS;
		case ExecutorBenchmarkType::VARIADIC_EXECUTE:
			return constant_mask == 0xF ? EXECUTOR_CONSTANT_BENCHMARK_ITERATIONS : EXECUTOR_BENCHMARK_ITERATIONS;
		}
		throw InternalException("Unknown executor benchmark type");
	}

	ExecutorBenchmarkType type;
	uint8_t constant_mask;
	uint8_t dictionary_mask;
	ExecutorNullProfile null_profile;
	idx_t selection_percent;
	ExecutorSelectionOutput selection_output;
};

ExecutorBenchmark unary_flat("ScalarExecutorUnaryFlat", ExecutorBenchmarkType::UNARY_EXECUTE);
ExecutorBenchmark unary_constant("ScalarExecutorUnaryConstant", ExecutorBenchmarkType::UNARY_EXECUTE, 0x1);
ExecutorBenchmark unary_dictionary("ScalarExecutorUnaryDictionary", ExecutorBenchmarkType::UNARY_EXECUTE, 0, true);

ExecutorBenchmark binary_flat_flat("ScalarExecutorBinaryFlatFlat", ExecutorBenchmarkType::BINARY_EXECUTE);
ExecutorBenchmark binary_constant_flat("ScalarExecutorBinaryConstantFlat", ExecutorBenchmarkType::BINARY_EXECUTE, 0x1);
ExecutorBenchmark binary_flat_constant("ScalarExecutorBinaryFlatConstant", ExecutorBenchmarkType::BINARY_EXECUTE, 0x2);
ExecutorBenchmark binary_constant_constant("ScalarExecutorBinaryConstantConstant",
                                           ExecutorBenchmarkType::BINARY_EXECUTE, 0x3);
ExecutorBenchmark binary_dictionary_flat("ScalarExecutorBinaryDictionaryFlat", ExecutorBenchmarkType::BINARY_EXECUTE, 0,
                                         0x1);
ExecutorBenchmark binary_flat_dictionary("ScalarExecutorBinaryFlatDictionary", ExecutorBenchmarkType::BINARY_EXECUTE, 0,
                                         0x2);
ExecutorBenchmark binary_dictionary_dictionary("ScalarExecutorBinaryDictionaryDictionary",
                                               ExecutorBenchmarkType::BINARY_EXECUTE, 0, 0x3);
ExecutorBenchmark binary_dictionary_flat_sparse_null("ScalarExecutorBinaryDictionaryFlatNull01",
                                                     ExecutorBenchmarkType::BINARY_EXECUTE, 0, 0x1,
                                                     ExecutorNullProfile::SPARSE);
ExecutorBenchmark binary_flat_dictionary_sparse_null("ScalarExecutorBinaryFlatDictionaryNull01",
                                                     ExecutorBenchmarkType::BINARY_EXECUTE, 0, 0x2,
                                                     ExecutorNullProfile::SPARSE);
ExecutorBenchmark binary_dictionary_dictionary_sparse_null("ScalarExecutorBinaryDictionaryDictionaryNull01",
                                                           ExecutorBenchmarkType::BINARY_EXECUTE, 0, 0x3,
                                                           ExecutorNullProfile::SPARSE);
ExecutorBenchmark binary_select_flat_flat("ScalarExecutorBinarySelectFlatFlat", ExecutorBenchmarkType::BINARY_SELECT);
ExecutorBenchmark binary_select_flat_constant("ScalarExecutorBinarySelectFlatConstant",
                                              ExecutorBenchmarkType::BINARY_SELECT, 0x2);
ExecutorBenchmark binary_select_dictionary_constant("ScalarExecutorBinarySelectDictionaryConstant",
                                                    ExecutorBenchmarkType::BINARY_SELECT, 0x2, 0x1);
ExecutorBenchmark binary_select_constant_dictionary("ScalarExecutorBinarySelectConstantDictionary",
                                                    ExecutorBenchmarkType::BINARY_SELECT, 0x1, 0x2);
ExecutorBenchmark
    binary_select_greater_equals_dictionary_constant("ScalarExecutorBinarySelectGreaterEqualsDictionaryConstant",
                                                     ExecutorBenchmarkType::BINARY_SELECT_GREATER_THAN_EQUALS, 0x2,
                                                     0x1);
ExecutorBenchmark ternary_select_flat("ScalarExecutorTernarySelectFlat", ExecutorBenchmarkType::TERNARY_SELECT);
ExecutorBenchmark ternary_select_fcc_true("ScalarExecutorTernarySelectFlatConstantConstantTrueOnly50",
                                          ExecutorBenchmarkType::TERNARY_SELECT, 0x5, 0, ExecutorNullProfile::NONE, 50,
                                          ExecutorSelectionOutput::TRUE_ONLY);
ExecutorBenchmark ternary_select_fcc_true_sparse_null("ScalarExecutorTernarySelectFlatConstantConstantTrueOnlyNull01",
                                                      ExecutorBenchmarkType::TERNARY_SELECT, 0x5, 0,
                                                      ExecutorNullProfile::SPARSE, 50,
                                                      ExecutorSelectionOutput::TRUE_ONLY);

ExecutorBenchmark ternary_fff("ScalarExecutorTernaryFFF", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x0);
ExecutorBenchmark ternary_cff("ScalarExecutorTernaryCFF", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x1);
ExecutorBenchmark ternary_fcf("ScalarExecutorTernaryFCF", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x2);
ExecutorBenchmark ternary_ccf("ScalarExecutorTernaryCCF", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x3);
ExecutorBenchmark ternary_ffc("ScalarExecutorTernaryFFC", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x4);
ExecutorBenchmark ternary_cfc("ScalarExecutorTernaryCFC", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x5);
ExecutorBenchmark ternary_fcc("ScalarExecutorTernaryFCC", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x6);
ExecutorBenchmark ternary_ccc("ScalarExecutorTernaryCCC", ExecutorBenchmarkType::TERNARY_EXECUTE, 0x7);

ExecutorBenchmark unary_sparse_null("ScalarExecutorUnaryFlatNull01", ExecutorBenchmarkType::UNARY_EXECUTE, 0, false,
                                    ExecutorNullProfile::SPARSE);
ExecutorBenchmark unary_dense_null("ScalarExecutorUnaryFlatNull50", ExecutorBenchmarkType::UNARY_EXECUTE, 0, false,
                                   ExecutorNullProfile::DENSE);
ExecutorBenchmark binary_sparse_null("ScalarExecutorBinaryFlatFlatNull01", ExecutorBenchmarkType::BINARY_EXECUTE, 0,
                                     false, ExecutorNullProfile::SPARSE);
ExecutorBenchmark binary_clustered_null("ScalarExecutorBinaryFlatFlatNullClustered",
                                        ExecutorBenchmarkType::BINARY_EXECUTE, 0, false,
                                        ExecutorNullProfile::CLUSTERED);
ExecutorBenchmark ternary_sparse_null("ScalarExecutorTernaryFFFNull01", ExecutorBenchmarkType::TERNARY_EXECUTE, 0,
                                      false, ExecutorNullProfile::SPARSE);
ExecutorBenchmark ternary_dense_null("ScalarExecutorTernaryFFFNull50", ExecutorBenchmarkType::TERNARY_EXECUTE, 0, false,
                                     ExecutorNullProfile::DENSE);
ExecutorBenchmark ternary_mostly_null("ScalarExecutorTernaryFFFNull99", ExecutorBenchmarkType::TERNARY_EXECUTE, 0,
                                      false, ExecutorNullProfile::MOSTLY_NULL);
ExecutorBenchmark ternary_clustered_null("ScalarExecutorTernaryFFFNullClustered",
                                         ExecutorBenchmarkType::TERNARY_EXECUTE, 0, false,
                                         ExecutorNullProfile::CLUSTERED);
ExecutorBenchmark ternary_optional("ScalarExecutorTernaryOptional", ExecutorBenchmarkType::TERNARY_OPTIONAL_EXECUTE);

ExecutorBenchmark select_none("ScalarExecutorBinarySelectRate00", ExecutorBenchmarkType::BINARY_SELECT, 0, false,
                              ExecutorNullProfile::NONE, 0);
ExecutorBenchmark select_sparse("ScalarExecutorBinarySelectRate01", ExecutorBenchmarkType::BINARY_SELECT, 0, false,
                                ExecutorNullProfile::NONE, 1);
ExecutorBenchmark select_half("ScalarExecutorBinarySelectRate50", ExecutorBenchmarkType::BINARY_SELECT, 0, false,
                              ExecutorNullProfile::NONE, 50);
ExecutorBenchmark select_most("ScalarExecutorBinarySelectRate99", ExecutorBenchmarkType::BINARY_SELECT, 0, false,
                              ExecutorNullProfile::NONE, 99);
ExecutorBenchmark select_half_true("ScalarExecutorBinarySelectRate50True", ExecutorBenchmarkType::BINARY_SELECT, 0,
                                   false, ExecutorNullProfile::NONE, 50, ExecutorSelectionOutput::TRUE_ONLY);
ExecutorBenchmark select_half_false("ScalarExecutorBinarySelectRate50False", ExecutorBenchmarkType::BINARY_SELECT, 0,
                                    false, ExecutorNullProfile::NONE, 50, ExecutorSelectionOutput::FALSE_ONLY);

ExecutorBenchmark variadic_flat("ScalarExecutorVariadicFlat", ExecutorBenchmarkType::VARIADIC_EXECUTE);
ExecutorBenchmark variadic_dictionary("ScalarExecutorVariadicDictionary", ExecutorBenchmarkType::VARIADIC_EXECUTE, 0,
                                      true);

} // namespace
