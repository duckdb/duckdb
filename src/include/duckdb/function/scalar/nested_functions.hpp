//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"

namespace duckdb {

enum class MapInvalidReason : uint8_t { VALID, NULL_KEY_LIST, NULL_KEY, DUPLICATE_KEY };
enum class UnionInvalidReason : uint8_t { VALID, TAG_OUT_OF_RANGE, NO_MEMBERS, VALIDITY_OVERLAP };

struct ListArgFunctor {
	static Vector &GetList(Vector &list) {
		return list;
	}
	static idx_t GetListSize(Vector &list) {
		return ListVector::GetListSize(list);
	}
	static Vector &GetEntry(Vector &list) {
		return ListVector::GetEntry(list);
	}
};

struct MapKeyArgFunctor {
	// MAP is a LIST(STRUCT(K,V))
	// meaning the MAP itself is a List, but the child vector that we're interested in (the keys)
	// are a level deeper than the initial child vector

	static Vector &GetList(Vector &map) {
		return map;
	}
	static idx_t GetListSize(Vector &map) {
		return ListVector::GetListSize(map);
	}
	static Vector &GetEntry(Vector &map) {
		return MapVector::GetKeys(map);
	}
};

struct ContainsFunctor {
	static inline bool Initialize() {
		return false;
	}
	static inline bool UpdateResultEntries(idx_t child_idx) {
		return true;
	}
};

struct PositionFunctor {
	static inline int32_t Initialize() {
		return 0;
	}
	static inline int32_t UpdateResultEntries(idx_t child_idx) {
		return child_idx + 1;
	}
};

struct VariableReturnBindData : public FunctionData {
	LogicalType stype;

	explicit VariableReturnBindData(LogicalType stype_p) : stype(std::move(stype_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VariableReturnBindData>(stype);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const VariableReturnBindData &)other_p;
		return stype == other.stype;
	}

	static void Serialize(FieldWriter &writer, const FunctionData *bind_data_p, const ScalarFunction &function) {
		D_ASSERT(bind_data_p);
		auto &info = bind_data_p->Cast<VariableReturnBindData>();
		writer.WriteSerializable(info.stype);
	}

	static unique_ptr<FunctionData> Deserialize(ClientContext &context, FieldReader &reader,
	                                            ScalarFunction &bound_function) {
		auto stype = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		return make_uniq<VariableReturnBindData>(std::move(stype));
	}
};

template <class T, class MAP_TYPE = map<T, idx_t>>
struct HistogramAggState {
	MAP_TYPE *hist;
};

struct ArraySliceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructPackFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructInsertFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListValueFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListRangeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapFromEntriesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapEntriesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapValuesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapKeysFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnionValueFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnionExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnionTagFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListTransformFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListFilterFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListConcatFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListContainsFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListFlattenFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListPositionFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListAggregateFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListDistinctFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListUniqueFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListSortFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CardinalityFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructExtractFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

MapInvalidReason CheckMapValidity(Vector &map, idx_t count,
                                  const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
void MapConversionVerify(Vector &vector, idx_t count);

UnionInvalidReason CheckUnionValidity(Vector &vector, idx_t count,
                                      const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());

} // namespace duckdb
