#include "duckdb/planner/operator/logical_column_data_get.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"

namespace duckdb {

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection)) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, chunk_types.size());
}

void LogicalColumnDataGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(chunk_types);
	writer.WriteField(collection->ChunkCount());
	for (auto &chunk : collection->Chunks()) {
		chunk.Serialize(writer.GetSerializer());
	}
}

unique_ptr<LogicalOperator> LogicalColumnDataGet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto chunk_count = reader.ReadRequired<idx_t>();
	auto collection = make_uniq<ColumnDataCollection>(state.gstate.context, chunk_types);
	for (idx_t i = 0; i < chunk_count; i++) {
		DataChunk chunk;
		chunk.Deserialize(reader.GetSource());
		collection->Append(chunk);
	}
	return make_uniq<LogicalColumnDataGet>(table_index, std::move(chunk_types), std::move(collection));
}

vector<idx_t> LogicalColumnDataGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

CKeyCollection *LogicalColumnDataGet::DeriveKeyCollection(CExpressionHandle &expression_handle) {
	vector<ColumnBinding> v = GenerateColumnBindings(table_index, chunk_types.size());
	return new CKeyCollection(v);
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalColumnDataGet::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalColumnDataGet::DerivePropertyConstraint(CExpressionHandle &expression_handle) {
	return nullptr;
	// return PpcDeriveConstraintPassThru(expression_handle, 0);
}

// Rehydrate expression from a given cost context and child expressions
Operator *LogicalColumnDataGet::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                              CDrvdPropCtxtPlan *pdpctxtplan) {
	LogicalColumnDataGet *pexpr =
	    new LogicalColumnDataGet(table_index, chunk_types, make_uniq<ColumnDataCollection>(*collection));
	pexpr->m_cost = pcc->m_cost;
	pexpr->m_group_expression = pcc->m_group_expression;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalColumnDataGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXform_set *LogicalColumnDataGet::PxfsCandidates() const {
	CXform_set *xform_set = new CXform_set();
	(void)xform_set->set(CXform::ExfImplementColumnDataGet);
	return xform_set;
}
} // namespace duckdb