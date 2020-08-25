#pragma once

#include "config.h"
#include "dss.h"

#ifdef __cplusplus
extern "C" {
#endif

DSS_HUGE AdvanceRand64(DSS_HUGE nSeed, DSS_HUGE nCount);
void dss_random64(DSS_HUGE *tgt, DSS_HUGE nLow, DSS_HUGE nHigh, long stream);
DSS_HUGE NextRand64(DSS_HUGE nSeed);

#ifdef __cplusplus
};
#endif
