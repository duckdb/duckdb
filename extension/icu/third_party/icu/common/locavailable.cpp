// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
*
*   Copyright (C) 1997-2013, International Business Machines
*   Corporation and others.  All Rights Reserved.
*
*******************************************************************************
*   file name:  locavailable.cpp
*   encoding:   UTF-8
*   tab size:   8 (not used)
*   indentation:4
*
*   created on: 2010feb25
*   created by: Markus W. Scherer
*
*   Code for available locales, separated out from other .cpp files
*   that then do not depend on resource bundle code and res_index bundles.
*/

#include "unicode/errorcode.h"
#include "unicode/utypes.h"
#include "unicode/locid.h"
#include "unicode/uloc.h"
#include "unicode/ures.h"
#include "cmemory.h"
#include "cstring.h"
#include "ucln_cmn.h"
#include "uassert.h"
#include "umutex.h"
#include "uresimp.h"

// C++ API ----------------------------------------------------------------- ***

U_NAMESPACE_BEGIN

static icu::Locale*  gLocAvailable_availableLocaleList = NULL;
static int32_t  gLocAvailable_availableLocaleListCount;
static icu::UInitOnce gLocAvailableInitOnceLocale = U_INITONCE_INITIALIZER;

U_NAMESPACE_END

U_CDECL_BEGIN

static UBool U_CALLCONV locale_available_cleanup(void)
{
    U_NAMESPACE_USE

    if (gLocAvailable_availableLocaleList) {
        delete []gLocAvailable_availableLocaleList;
        gLocAvailable_availableLocaleList = NULL;
    }
    gLocAvailable_availableLocaleListCount = 0;
    gLocAvailableInitOnceLocale.reset();

    return TRUE;
}

U_CDECL_END

U_NAMESPACE_BEGIN

void U_CALLCONV locale_available_init() {
    // This function is a friend of class Locale.
    // This function is only invoked via umtx_initOnce().

    // for now, there is a hardcoded list, so just walk through that list and set it up.
    //  Note: this function is a friend of class Locale.
    gLocAvailable_availableLocaleListCount = uloc_countAvailable();
    if(gLocAvailable_availableLocaleListCount) {
       gLocAvailable_availableLocaleList = new Locale[gLocAvailable_availableLocaleListCount];
    }
    if (gLocAvailable_availableLocaleList == NULL) {
        gLocAvailable_availableLocaleListCount= 0;
    }
    for (int32_t locCount=gLocAvailable_availableLocaleListCount-1; locCount>=0; --locCount) {
        gLocAvailable_availableLocaleList[locCount].setFromPOSIXID(uloc_getAvailable(locCount));
    }
    ucln_common_registerCleanup(UCLN_COMMON_LOCALE_AVAILABLE, locale_available_cleanup);
}

const Locale* U_EXPORT2
Locale::getAvailableLocales(int32_t& count)
{
    umtx_initOnce(gLocAvailableInitOnceLocale, &locale_available_init);
    count = gLocAvailable_availableLocaleListCount;
    return gLocAvailable_availableLocaleList;
}


U_NAMESPACE_END

// C API ------------------------------------------------------------------- ***

U_NAMESPACE_USE

/* ### Constants **************************************************/

namespace {

// Enough capacity for the two lists in the res_index.res file
const char** gAvailableLocaleNames[2] = {};
int32_t gAvailableLocaleCounts[2] = {};
icu::UInitOnce ginstalledLocalesInitOnce = U_INITONCE_INITIALIZER;

class AvailableLocalesSink : public ResourceSink {
  public:
    void put(const char *key, ResourceValue &value, UBool /*noFallback*/, UErrorCode &status) U_OVERRIDE {
        ResourceTable resIndexTable = value.getTable(status);
        if (U_FAILURE(status)) {
            return;
        }
        for (int32_t i = 0; resIndexTable.getKeyAndValue(i, key, value); ++i) {
            ULocAvailableType type;
            if (uprv_strcmp(key, "InstalledLocales") == 0) {
                type = ULOC_AVAILABLE_DEFAULT;
            } else if (uprv_strcmp(key, "AliasLocales") == 0) {
                type = ULOC_AVAILABLE_ONLY_LEGACY_ALIASES;
            } else {
                // CLDRVersion, etc.
                continue;
            }
            ResourceTable availableLocalesTable = value.getTable(status);
            if (U_FAILURE(status)) {
                return;
            }
            gAvailableLocaleCounts[type] = availableLocalesTable.getSize();
            gAvailableLocaleNames[type] = static_cast<const char**>(
                uprv_malloc(gAvailableLocaleCounts[type] * sizeof(const char*)));
            if (gAvailableLocaleNames[type] == nullptr) {
                status = U_MEMORY_ALLOCATION_ERROR;
                return;
            }
            for (int32_t j = 0; availableLocalesTable.getKeyAndValue(j, key, value); ++j) {
                gAvailableLocaleNames[type][j] = key;
            }
        }
    }
};

class AvailableLocalesStringEnumeration : public StringEnumeration {
  public:
    AvailableLocalesStringEnumeration(ULocAvailableType type) : fType(type) {
    }

    const char* next(int32_t *resultLength, UErrorCode&) override {
        ULocAvailableType actualType = fType;
        int32_t actualIndex = fIndex++;

        // If the "combined" list was requested, resolve that now
        if (fType == ULOC_AVAILABLE_WITH_LEGACY_ALIASES) {
            int32_t defaultLocalesCount = gAvailableLocaleCounts[ULOC_AVAILABLE_DEFAULT];
            if (actualIndex < defaultLocalesCount) {
                actualType = ULOC_AVAILABLE_DEFAULT;
            } else {
                actualIndex -= defaultLocalesCount;
                actualType = ULOC_AVAILABLE_ONLY_LEGACY_ALIASES;
            }
        }

        // Return the requested string
        int32_t count = gAvailableLocaleCounts[actualType];
        const char* result;
        if (actualIndex < count) {
            result = gAvailableLocaleNames[actualType][actualIndex];
            if (resultLength != nullptr) {
                *resultLength = static_cast<int32_t>(uprv_strlen(result));
            }
        } else {
            result = nullptr;
            if (resultLength != nullptr) {
                *resultLength = 0;
            }
        }
        return result;
    }

    void reset(UErrorCode&) override {
        fIndex = 0;
    }

    int32_t count(UErrorCode&) const override {
        if (fType == ULOC_AVAILABLE_WITH_LEGACY_ALIASES) {
            return gAvailableLocaleCounts[ULOC_AVAILABLE_DEFAULT]
                + gAvailableLocaleCounts[ULOC_AVAILABLE_ONLY_LEGACY_ALIASES];
        } else {
            return gAvailableLocaleCounts[fType];
        }
    }

  private:
    ULocAvailableType fType;
    int32_t fIndex = 0;
};

/* ### Get available **************************************************/

static UBool U_CALLCONV uloc_cleanup(void) {
    for (int32_t i = 0; i < UPRV_LENGTHOF(gAvailableLocaleNames); i++) {
        uprv_free(gAvailableLocaleNames[i]);
        gAvailableLocaleNames[i] = nullptr;
        gAvailableLocaleCounts[i] = 0;
    }
    ginstalledLocalesInitOnce.reset();
    return TRUE;
}

// Load Installed Locales. This function will be called exactly once
//   via the initOnce mechanism.

static void U_CALLCONV loadInstalledLocales(UErrorCode& status) {
    ucln_common_registerCleanup(UCLN_COMMON_ULOC, uloc_cleanup);

    icu::LocalUResourceBundlePointer rb(ures_openDirect(NULL, "res_index", &status));
    AvailableLocalesSink sink;
    ures_getAllItemsWithFallback(rb.getAlias(), "", sink, status);
}

void _load_installedLocales(UErrorCode& status) {
    umtx_initOnce(ginstalledLocalesInitOnce, &loadInstalledLocales, status);
}

} // namespace

U_CAPI const char* U_EXPORT2
uloc_getAvailable(int32_t offset) {
    icu::ErrorCode status;
    _load_installedLocales(status);
    if (status.isFailure()) {
        return nullptr;
    }
    if (offset > gAvailableLocaleCounts[0]) {
        // *status = U_ILLEGAL_ARGUMENT_ERROR;
        return nullptr;
    }
    return gAvailableLocaleNames[0][offset];
}

U_CAPI int32_t  U_EXPORT2
uloc_countAvailable() {
    icu::ErrorCode status;
    _load_installedLocales(status);
    if (status.isFailure()) {
        return 0;
    }
    return gAvailableLocaleCounts[0];
}

U_CAPI UEnumeration* U_EXPORT2
uloc_openAvailableByType(ULocAvailableType type, UErrorCode* status) {
    if (U_FAILURE(*status)) {
        return nullptr;
    }
    if (type < 0 || type >= ULOC_AVAILABLE_COUNT) {
        *status = U_ILLEGAL_ARGUMENT_ERROR;
        return nullptr;
    }
    _load_installedLocales(*status);
    if (U_FAILURE(*status)) {
        return nullptr;
    }
    LocalPointer<AvailableLocalesStringEnumeration> result(
        new AvailableLocalesStringEnumeration(type), *status);
    if (U_FAILURE(*status)) {
        return nullptr;
    }
    return uenum_openFromStringEnumeration(result.orphan(), status);
}

