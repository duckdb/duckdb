// © 2018 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html

// This file contains utilities to deal with static-allocated UnicodeSets.
//
// Common use case: you write a "private static final" UnicodeSet in Java, and
// want something similarly easy in C++.  Originally written for number
// parsing, but this header can be used for other applications.
//
// Main entrypoint: `unisets::get(unisets::MY_SET_ID_HERE)`
//
// This file is in common instead of i18n because it is needed by ucurr.cpp.
//
// Author: sffc

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING
#ifndef __STATIC_UNICODE_SETS_H__
#define __STATIC_UNICODE_SETS_H__

#include "unicode/uniset.h"
#include "unicode/unistr.h"

U_NAMESPACE_BEGIN
namespace unisets {

enum Key {
    // UNISET_KEY_NONE is used to indicate null in chooseFrom().
    // UNISET_KEY_EMPTY is used to get an empty UnicodeSet.
    UNISET_KEY_NONE = -1,
    UNISET_KEY_EMPTY = 0,

    // Ignorables
    UNISET_KEY_DEFAULT_IGNORABLES,
    UNISET_KEY_STRICT_IGNORABLES,

    // Separators
    // Notes:
    // - UNISET_KEY_COMMA is a superset of UNISET_KEY_STRICT_COMMA
    // - UNISET_KEY_PERIOD is a superset of SCRICT_UNISET_KEY_PERIOD
    // - UNISET_KEY_ALL_SEPARATORS is the union of UNISET_KEY_COMMA, UNISET_KEY_PERIOD, and UNISET_KEY_OTHER_GROUPING_SEPARATORS
    // - UNISET_KEY_STRICT_ALL_SEPARATORS is the union of UNISET_KEY_STRICT_COMMA, UNISET_KEY_STRICT_PERIOD, and OTHER_GRP_SEPARATORS
    UNISET_KEY_COMMA,
    UNISET_KEY_PERIOD,
    UNISET_KEY_STRICT_COMMA,
    UNISET_KEY_STRICT_PERIOD,
    UNISET_KEY_APOSTROPHE_SIGN,
    UNISET_KEY_OTHER_GROUPING_SEPARATORS,
    UNISET_KEY_ALL_SEPARATORS,
    UNISET_KEY_STRICT_ALL_SEPARATORS,

    // Symbols
    UNISET_KEY_MINUS_SIGN,
    UNISET_KEY_PLUS_SIGN,
    UNISET_KEY_PERCENT_SIGN,
    UNISET_KEY_PERMILLE_SIGN,
    UNISET_KEY_INFINITY_SIGN,

    // Currency Symbols
    UNISET_KEY_DOLLAR_SIGN,
    UNISET_KEY_POUND_SIGN,
    UNISET_KEY_RUPEE_SIGN,
    UNISET_KEY_YEN_SIGN,
    UNISET_KEY_WON_SIGN,

    // Other
    UNISET_KEY_DIGITS,

    // Combined Separators with Digits (for lead code points)
    UNISET_KEY_DIGITS_OR_ALL_SEPARATORS,
    UNISET_KEY_DIGITS_OR_STRICT_ALL_SEPARATORS,

    // The number of elements in the enum.
    UNISET_KEY_UNISETS_KEY_COUNT
};

/**
 * Gets the static-allocated UnicodeSet according to the provided key. The
 * pointer will be deleted during u_cleanup(); the caller should NOT delete it.
 *
 * Exported as U_COMMON_API for ucurr.cpp
 *
 * This method is always safe and OK to chain: in the case of a memory or other
 * error, it returns an empty set from static memory.
 *
 * Example:
 *
 *     UBool hasIgnorables = unisets::get(unisets::UNISET_KEY_DEFAULT_IGNORABLES)->contains(...);
 *
 * @param key The desired UnicodeSet according to the enum in this file.
 * @return The requested UnicodeSet. Guaranteed to be frozen and non-null, but
 *         may be empty if an error occurred during data loading.
 */
U_COMMON_API const UnicodeSet* get(Key key);

/**
 * Checks if the UnicodeSet given by key1 contains the given string.
 *
 * Exported as U_COMMON_API for numparse_decimal.cpp
 *
 * @param str The string to check.
 * @param key1 The set to check.
 * @return key1 if the set contains str, or UNISET_KEY_NONE if not.
 */
U_COMMON_API Key chooseFrom(UnicodeString str, Key key1);

/**
 * Checks if the UnicodeSet given by either key1 or key2 contains the string.
 *
 * Exported as U_COMMON_API for numparse_decimal.cpp
 *
 * @param str The string to check.
 * @param key1 The first set to check.
 * @param key2 The second set to check.
 * @return key1 if that set contains str; key2 if that set contains str; or
 *         UNISET_KEY_NONE if neither set contains str.
 */
U_COMMON_API Key chooseFrom(UnicodeString str, Key key1, Key key2);

// TODO: Load these from data: ICU-20108
// Unused in C++:
// Key chooseCurrency(UnicodeString str);
// Used instead:
static const struct {
    Key key;
    UChar32 exemplar;
} kCurrencyEntries[] = {
	{UNISET_KEY_DOLLAR_SIGN, u'\x24'}, // u'$'
	{UNISET_KEY_POUND_SIGN, u'\xa3'}, // u'£'
	{UNISET_KEY_RUPEE_SIGN, u'\x20b9'}, // u'₹'
	{UNISET_KEY_YEN_SIGN, u'\xa5'}, // u'¥'
	{UNISET_KEY_WON_SIGN, u'\x20a9'}, // u'₩'
};

} // namespace unisets
U_NAMESPACE_END

#endif //__STATIC_UNICODE_SETS_H__
#endif /* #if !UCONFIG_NO_FORMATTING */
