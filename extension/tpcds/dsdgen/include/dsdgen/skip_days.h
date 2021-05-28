#ifndef R_SKIP_DAYS_H
#define R_SKIP_DAYS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "config.h"
#include "porting.h"

ds_key_t skipDays(int nTable, ds_key_t *pRemainder);

#ifdef __cplusplus
};
#endif

#endif
