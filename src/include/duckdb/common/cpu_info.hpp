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
	const vector<CPUFeature> &GetAvailInstructionSets() const;
    CPUFeature GetBestFeature() const;
	void Initialize();
public:
	CpuInfo();
};
}

