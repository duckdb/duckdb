#include "duckdb/common/cpu_info.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

#if defined(DUCKDB_X86_64)
    #  if defined(_MSC_VER)
    static void DUCKDB_cpu_getid(int func, int* data) {
      __cpuid(data, func);
    }
    #  else
    static void CpuGetid(int func, int* data) {
        __asm__ ("cpuid"
        : "=a" (data[0]), "=b" (data[1]), "=c" (data[2]), "=d" (data[3])
        : "0" (func), "2" (0));
    }
    #  endif
#elif defined(DUCKDB_ARM)
    #if (defined(__GNUC__) && ((__GNUC__ > 2) || (__GNUC__ == 2 && __GNUC_MINOR__ >= 16)))
    #    define PSNIP_CPU__IMPL_GETAUXVAL
    #    include <sys/auxv.h>
    #endif
#endif


#if defined(DUCKDB_X86_64)
static unsigned int duckdb_cpuinfo[8 * 4] = { 0, };
#elif defined(DUCKDB_ARM)
static unsigned long DUCKDB_cpuinfo[2] = { 0, };
#endif

static void CpuInit(void) {
#if defined(DUCKDB_X86_64)
    int i;
    for (i = 0 ; i < 8 ; i++) {
        CpuGetid(i, (int*) &(duckdb_cpuinfo[i * 4]));
    }
#elif defined(DUCKDB_ARM)
    DUCKDB_cpuinfo[0] = getauxval (AT_HWCAP);
  DUCKDB_cpuinfo[1] = getauxval (AT_HWCAP2);
#endif
}

static int CpuFeatureCheck (enum CPUFeature feature) {
#if defined(DUCKDB_X86_64)
    unsigned int i, r, b;
#elif defined(DUCKDB_ARM)
    unsigned long b, i;
#endif

#if defined(DUCKDB_X86_64)
    if ((feature & DUCKDB_CPU_FEATURE_CPU_MASK) != DUCKDB_CPU_FEATURE_X86) {
		return 0;
	}
#elif defined(DUCKDB_ARM)
    if ((feature & PSNIP_CPU_FEATURE_CPU_MASK) != PSNIP_CPU_FEATURE_ARM) {
		return 0;
	}
#else
  return 0;
#endif

    feature = static_cast<CPUFeature>(~DUCKDB_CPU_FEATURE_CPU_MASK & feature);
#if defined(_MSC_VER)
    #pragma warning(push)
#pragma warning(disable:4152)
#endif
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#if defined(DUCKDB_X86_64)
    i = (feature >> 16) & 0xff;
    r = (feature >>  8) & 0xff;
    b = (feature      ) & 0xff;

    if (i > 7 || r > 3 || b > 31) {
		return 0;
	}

    return (duckdb_cpuinfo[(i * 4) + r] >> b) & 1;
#elif defined(DUCKDB_ARM)
    b = 1 << ((feature & 0xff) - 1);
  i = DUCKDB_cpuinfo[(feature >> 0x08) & 0xff];
  return (DUCKDB_cpuinfo[(feature >> 0x08) & 0xff] & b) == b;
#endif
}

CpuInfo::CpuInfo() {
	Initialize();
}

const vector<CPUFeature> &CpuInfo::GetAvailInstructionSets() const {
	return avail_features;
}
void CpuInfo::Initialize() {
	// to make sure CpuFeatureCheck used at least once
    CpuFeatureCheck(DUCKDB_CPU_FALLBACK);
    best_feature = DUCKDB_CPU_FALLBACK;
    avail_features.push_back(DUCKDB_CPU_FALLBACK);

    CpuInit();

#ifdef DUCKDB_X86_64
	// Simple rule, the wider better.
    if (CpuFeatureCheck(DUCKDB_CPU_FEATURE_X86_AVX2)) {
		best_feature = DUCKDB_CPU_FEATURE_X86_AVX2;
		avail_features.push_back(DUCKDB_CPU_FEATURE_X86_AVX2);
	}
    if (CpuFeatureCheck(DUCKDB_CPU_FEATURE_X86_AVX512F)) {
        best_feature = DUCKDB_CPU_FEATURE_X86_AVX512F;
		avail_features.push_back(DUCKDB_CPU_FEATURE_X86_AVX512F);
    }
#elif defined(DUCKDB_ARM)
#endif
}
CPUFeature CpuInfo::GetBestFeature() const {
	return best_feature;
}


} // namespace duckdb
