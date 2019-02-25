// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// From Apache Impala (incubating) as of 2016-01-29.

#include "arrow/util/cpu-info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <stdlib.h>
#include <string.h>

#ifndef _MSC_VER
#include <unistd.h>
#endif

#ifdef _WIN32
#include <intrin.h>
#include <array>
#include <bitset>
#include "arrow/util/windows_compatibility.h"

#endif

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "arrow/util/logging.h"

using boost::algorithm::contains;
using boost::algorithm::trim;
using std::max;

namespace arrow {
namespace internal {

static struct {
  std::string name;
  int64_t flag;
} flag_mappings[] = {
    {"ssse3", CpuInfo::SSSE3},
    {"sse4_1", CpuInfo::SSE4_1},
    {"sse4_2", CpuInfo::SSE4_2},
    {"popcnt", CpuInfo::POPCNT},
};
static const int64_t num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

namespace {

// Helper function to parse for hardware flags.
// values contains a list of space-seperated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t ParseCPUFlags(const std::string& values) {
  int64_t flags = 0;
  for (int i = 0; i < num_flags; ++i) {
    if (contains(values, flag_mappings[i].name)) {
      flags |= flag_mappings[i].flag;
    }
  }
  return flags;
}

}  // namespace

#ifdef _WIN32
bool RetrieveCacheSize(int64_t* cache_sizes) {
  if (!cache_sizes) {
    return false;
  }
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer_position = nullptr;
  DWORD buffer_size = 0;
  DWORD offset = 0;
  typedef BOOL(WINAPI * GetLogicalProcessorInformationFuncPointer)(void*, void*);
  GetLogicalProcessorInformationFuncPointer func_pointer =
      (GetLogicalProcessorInformationFuncPointer)GetProcAddress(
          GetModuleHandle("kernel32"), "GetLogicalProcessorInformation");

  if (!func_pointer) {
    return false;
  }

  // Get buffer size
  if (func_pointer(buffer, &buffer_size) && GetLastError() != ERROR_INSUFFICIENT_BUFFER)
    return false;

  buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(buffer_size);

  if (!buffer || !func_pointer(buffer, &buffer_size)) {
    return false;
  }

  buffer_position = buffer;
  while (offset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= buffer_size) {
    if (RelationCache == buffer_position->Relationship) {
      PCACHE_DESCRIPTOR cache = &buffer_position->Cache;
      if (cache->Level >= 1 && cache->Level <= 3) {
        cache_sizes[cache->Level - 1] += cache->Size;
      }
    }
    offset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
    buffer_position++;
  }

  if (buffer) {
    free(buffer);
  }
  return true;
}

bool RetrieveCPUInfo(int64_t* hardware_flags, std::string* model_name) {
  if (!hardware_flags || !model_name) {
    return false;
  }
  const int register_ECX_id = 1;
  int highest_valid_id = 0;
  int highest_extended_valid_id = 0;
  std::bitset<32> features_ECX;
  std::array<int, 4> cpu_info;

  // Get highest valid id
  __cpuid(cpu_info.data(), 0);
  highest_valid_id = cpu_info[0];

  if (highest_valid_id <= register_ECX_id) return false;

  __cpuidex(cpu_info.data(), register_ECX_id, 0);
  features_ECX = cpu_info[2];

  // Get highest extended id
  __cpuid(cpu_info.data(), 0x80000000);
  highest_extended_valid_id = cpu_info[0];

  // Retrieve CPU model name
  if (highest_extended_valid_id >= 0x80000004) {
    model_name->clear();
    for (int i = 0x80000002; i <= 0x80000004; ++i) {
      __cpuidex(cpu_info.data(), i, 0);
      *model_name +=
          std::string(reinterpret_cast<char*>(cpu_info.data()), sizeof(cpu_info));
    }
  }

  if (features_ECX[9]) *hardware_flags |= CpuInfo::SSSE3;
  if (features_ECX[19]) *hardware_flags |= CpuInfo::SSE4_1;
  if (features_ECX[20]) *hardware_flags |= CpuInfo::SSE4_2;
  if (features_ECX[23]) *hardware_flags |= CpuInfo::POPCNT;
  return true;
}
#endif

CpuInfo::CpuInfo() : hardware_flags_(0), num_cores_(1), model_name_("unknown") {}

std::unique_ptr<CpuInfo> g_cpu_info;
static std::mutex cpuinfo_mutex;

CpuInfo* CpuInfo::GetInstance() {
  std::lock_guard<std::mutex> lock(cpuinfo_mutex);
  if (!g_cpu_info) {
    g_cpu_info.reset(new CpuInfo);
    g_cpu_info->Init();
  }
  return g_cpu_info.get();
}

void CpuInfo::Init() {
  std::string line;
  std::string name;
  std::string value;

  float max_mhz = 0;
  int num_cores = 0;

  memset(&cache_sizes_, 0, sizeof(cache_sizes_));

#ifdef _WIN32
  SYSTEM_INFO system_info;
  GetSystemInfo(&system_info);
  num_cores = system_info.dwNumberOfProcessors;

  LARGE_INTEGER performance_frequency;
  if (QueryPerformanceFrequency(&performance_frequency)) {
    max_mhz = static_cast<float>(performance_frequency.QuadPart);
  }
#else
  // Read from /proc/cpuinfo
  std::ifstream cpuinfo("/proc/cpuinfo", std::ios::in);
  while (cpuinfo) {
    getline(cpuinfo, line);
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
      name = line.substr(0, colon - 1);
      value = line.substr(colon + 1, std::string::npos);
      trim(name);
      trim(value);
      if (name.compare("flags") == 0) {
        hardware_flags_ |= ParseCPUFlags(value);
      } else if (name.compare("cpu MHz") == 0) {
        // Every core will report a different speed.  We'll take the max, assuming
        // that when impala is running, the core will not be in a lower power state.
        // TODO: is there a more robust way to do this, such as
        // Window's QueryPerformanceFrequency()
        float mhz = static_cast<float>(atof(value.c_str()));
        max_mhz = max(mhz, max_mhz);
      } else if (name.compare("processor") == 0) {
        ++num_cores;
      } else if (name.compare("model name") == 0) {
        model_name_ = value;
      }
    }
  }
  if (cpuinfo.is_open()) cpuinfo.close();
#endif

#ifdef __APPLE__
  // On Mac OS X use sysctl() to get the cache sizes
  size_t len = 0;
  sysctlbyname("hw.cachesize", NULL, &len, NULL, 0);
  uint64_t* data = static_cast<uint64_t*>(malloc(len));
  sysctlbyname("hw.cachesize", data, &len, NULL, 0);
  DCHECK_GE(len / sizeof(uint64_t), 3);
  for (size_t i = 0; i < 3; ++i) {
    cache_sizes_[i] = data[i];
  }
#elif _WIN32
  if (!RetrieveCacheSize(cache_sizes_)) {
    SetDefaultCacheSize();
  }
  RetrieveCPUInfo(&hardware_flags_, &model_name_);
#else
  SetDefaultCacheSize();
#endif

  if (max_mhz != 0) {
    cycles_per_ms_ = static_cast<int64_t>(max_mhz);
#ifndef _WIN32
    cycles_per_ms_ *= 1000;
#endif
  } else {
    cycles_per_ms_ = 1000000;
  }
  original_hardware_flags_ = hardware_flags_;

  if (num_cores > 0) {
    num_cores_ = num_cores;
  } else {
    num_cores_ = 1;
  }
}

void CpuInfo::VerifyCpuRequirements() {
  if (!IsSupported(CpuInfo::SSSE3)) {
    DCHECK(false) << "CPU does not support the Supplemental SSE3 instruction set";
  }
}

bool CpuInfo::CanUseSSE4_2() const {
#ifdef ARROW_USE_SIMD
  return IsSupported(CpuInfo::SSE4_2);
#else
  return false;
#endif
}

void CpuInfo::EnableFeature(int64_t flag, bool enable) {
  if (!enable) {
    hardware_flags_ &= ~flag;
  } else {
    // Can't turn something on that can't be supported
    DCHECK_NE(original_hardware_flags_ & flag, 0);
    hardware_flags_ |= flag;
  }
}

int64_t CpuInfo::hardware_flags() { return hardware_flags_; }

int64_t CpuInfo::CacheSize(CacheLevel level) { return cache_sizes_[level]; }

int64_t CpuInfo::cycles_per_ms() { return cycles_per_ms_; }

int CpuInfo::num_cores() { return num_cores_; }

std::string CpuInfo::model_name() { return model_name_; }

void CpuInfo::SetDefaultCacheSize() {
#ifndef _SC_LEVEL1_DCACHE_SIZE
  // Provide reasonable default values if no info
  cache_sizes_[0] = 32 * 1024;    // Level 1: 32k
  cache_sizes_[1] = 256 * 1024;   // Level 2: 256k
  cache_sizes_[2] = 3072 * 1024;  // Level 3: 3M
#else
  // Call sysconf to query for the cache sizes
  cache_sizes_[0] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
  cache_sizes_[1] = sysconf(_SC_LEVEL2_CACHE_SIZE);
  cache_sizes_[2] = sysconf(_SC_LEVEL3_CACHE_SIZE);
#endif
}

}  // namespace internal
}  // namespace arrow
