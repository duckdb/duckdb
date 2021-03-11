//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/cpu_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/cpu_feature.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class CpuInfo {
	vector<CPUFeature> avail_features;
	CPUFeature best_feature;

public:
	void SetBestFeature(CPUFeature bestFeature);

public:
	const vector<CPUFeature> &GetAvailFeatures() const;
	bool HasFeature(CPUFeature feature);
	bool HasFeature(const string &feature);
	CPUFeature GetBestFeature() const;
	void Initialize();

public:
	CpuInfo();
	void SetFeature(ClientContext &client_context, CPUFeature feature);
};
} // namespace duckdb
