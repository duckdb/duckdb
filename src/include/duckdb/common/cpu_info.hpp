//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/cpu_info.hpp
//
//
//===----------------------------------------------------------------------===//
#include "duckdb/common/types.hpp"
#include "duckdb/common/cpu_feature.hpp"
#pragma once
namespace duckdb {
class CpuInfo {
	vector<CPUFeature> avail_features;
    CPUFeature best_feature;


public:
	void SetBestFeature(CPUFeature bestFeature);
    shared_ptr<BuiltinFunctions> builtin_functions;
public:
	const vector<CPUFeature> &GetAvailFeatures() const;
	bool HasFeature(CPUFeature feature);
    bool HasFeature(const string& feature);
    CPUFeature GetBestFeature() const;
	void Initialize();
public:
	CpuInfo();
    void SetFeature(CPUFeature feature);
};
} // namespace duckdb

