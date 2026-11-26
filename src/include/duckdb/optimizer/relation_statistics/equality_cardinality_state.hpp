//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/equality_cardinality_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

struct EqualityDisjointSets {
public:
	idx_t Add() {
		auto result = parents.size();
		parents.push_back(result);
		return result;
	}

	idx_t Find(idx_t value) {
		if (parents[value] != value) {
			parents[value] = Find(parents[value]);
		}
		return parents[value];
	}

	void Merge(idx_t a, idx_t b) {
		a = Find(a);
		b = Find(b);
		if (a != b) {
			parents[MaxValue(a, b)] = MinValue(a, b);
		}
	}

public:
	vector<idx_t> parents;
};

struct EqualityCardinalityState {
public:
	explicit EqualityCardinalityState(vector<RelationStats> &relations) : relations(relations) {
	}

public:
	idx_t BindingIndex(ColumnBinding binding) const {
		D_ASSERT(binding.table_index.index < relations.size());
		D_ASSERT(binding.column_index.GetIndex() < relations[binding.table_index.index].columns.size());
		return offsets[binding.table_index.index] + binding.column_index.GetIndex();
	}

	ColumnBinding Normalize(ColumnBinding binding) {
		return bindings[aliases.Find(BindingIndex(binding))];
	}

	idx_t NormalizeColumn(idx_t relation, idx_t column) {
		return Normalize(ColumnBinding(TableIndex(relation), ProjectionIndex(column))).column_index.GetIndex();
	}

	support_identities_t OrientIdentities(const support_identities_t &identities, const vector<idx_t> &columns,
	                                      const vector<idx_t> &ordered_columns) const;
	support_identities_t DeduplicateIdentities(const vector<SupportIdentity> &identities) const;
	void NormalizeEvidence();

public:
	//! Normalization updates the caller's statistics in place before the model reads them.
	vector<RelationStats> &relations;
	column_binding_map_t<vector<JoinColumnSources>> sources;
	vector<idx_t> offsets;
	vector<ColumnBinding> bindings;
	EqualityDisjointSets aliases;
};

struct EqualityFactor {
	vector<idx_t> components;
	EqualitySupport support;
	map<idx_t, EqualitySupport> source_joints;
};

void ReconcileEqualitySupport(EqualitySupport &point, const EqualitySupport &other);
EqualitySupport SupportWithinTuple(EqualitySupport support, const EqualitySupport &tuple);

} // namespace duckdb
