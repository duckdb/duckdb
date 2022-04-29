// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/********************************************************************
 * COPYRIGHT:
 * Copyright (c) 1997-2011, International Business Machines Corporation and
 * others. All Rights Reserved.
 * Copyright (C) 2010 , Yahoo! Inc.
 ********************************************************************
 * File selectfmtimpl.h
 *
 *   Date        Name        Description
 *   11/11/09    kirtig      Finished first cut of implementation.
 *********************************************************************/


#ifndef SELFMTIMPL_H
#define SELFMTIMPL_H

#if !UCONFIG_NO_FORMATTING

#include "unicode/format.h"
#include "unicode/locid.h"
#include "unicode/parseerr.h"
#include "unicode/utypes.h"
#include "uvector.h"
#include "hash.h"

U_NAMESPACE_BEGIN

#define selfmtimpl_DOT               ((UChar)0x002E)
#define selfmtimpl_SINGLE_QUOTE      ((UChar)0x0027)
#define selfmtimpl_SLASH             ((UChar)0x002F)
#define selfmtimpl_BACKSLASH         ((UChar)0x005C)
#define selfmtimpl_SPACE             ((UChar)0x0020)
#define selfmtimpl_TAB               ((UChar)0x0009)
#define selfmtimpl_QUOTATION_MARK    ((UChar)0x0022)
#define selfmtimpl_ASTERISK          ((UChar)0x002A)
#define selfmtimpl_COMMA             ((UChar)0x002C)
#define selfmtimpl_HYPHEN            ((UChar)0x002D)
#define selfmtimpl_U_ZERO            ((UChar)0x0030)
#define selfmtimpl_U_ONE             ((UChar)0x0031)
#define selfmtimpl_U_TWO             ((UChar)0x0032)
#define selfmtimpl_U_THREE           ((UChar)0x0033)
#define selfmtimpl_U_FOUR            ((UChar)0x0034)
#define selfmtimpl_U_FIVE            ((UChar)0x0035)
#define selfmtimpl_U_SIX             ((UChar)0x0036)
#define selfmtimpl_U_SEVEN           ((UChar)0x0037)
#define selfmtimpl_U_EIGHT           ((UChar)0x0038)
#define selfmtimpl_U_NINE            ((UChar)0x0039)
#define selfmtimpl_COLON             ((UChar)0x003A)
#define selfmtimpl_SEMI_COLON        ((UChar)0x003B)
#define selfmtimpl_CAP_A             ((UChar)0x0041)
#define selfmtimpl_CAP_B             ((UChar)0x0042)
#define selfmtimpl_CAP_R             ((UChar)0x0052)
#define selfmtimpl_CAP_Z             ((UChar)0x005A)
#define selfmtimpl_LOWLINE           ((UChar)0x005F)
#define selfmtimpl_LEFTBRACE         ((UChar)0x007B)
#define selfmtimpl_RIGHTBRACE        ((UChar)0x007D)

#define selfmtimpl_LOW_A             ((UChar)0x0061)
#define selfmtimpl_LOW_B             ((UChar)0x0062)
#define selfmtimpl_LOW_C             ((UChar)0x0063)
#define selfmtimpl_LOW_D             ((UChar)0x0064)
#define selfmtimpl_LOW_E             ((UChar)0x0065)
#define selfmtimpl_LOW_F             ((UChar)0x0066)
#define selfmtimpl_LOW_G             ((UChar)0x0067)
#define selfmtimpl_LOW_H             ((UChar)0x0068)
#define selfmtimpl_LOW_I             ((UChar)0x0069)
#define selfmtimpl_LOW_J             ((UChar)0x006a)
#define selfmtimpl_LOW_K             ((UChar)0x006B)
#define selfmtimpl_LOW_L             ((UChar)0x006C)
#define selfmtimpl_LOW_M             ((UChar)0x006D)
#define selfmtimpl_LOW_N             ((UChar)0x006E)
#define selfmtimpl_LOW_O             ((UChar)0x006F)
#define selfmtimpl_LOW_P             ((UChar)0x0070)
#define selfmtimpl_LOW_Q             ((UChar)0x0071)
#define selfmtimpl_LOW_R             ((UChar)0x0072)
#define selfmtimpl_LOW_S             ((UChar)0x0073)
#define selfmtimpl_LOW_T             ((UChar)0x0074)
#define selfmtimpl_LOW_U             ((UChar)0x0075)
#define selfmtimpl_LOW_V             ((UChar)0x0076)
#define selfmtimpl_LOW_W             ((UChar)0x0077)
#define selfmtimpl_LOW_X             ((UChar)0x0078)
#define selfmtimpl_LOW_Y             ((UChar)0x0079)
#define selfmtimpl_LOW_Z             ((UChar)0x007A)

class UnicodeSet;

U_NAMESPACE_END

#endif /* #if !UCONFIG_NO_FORMATTING */

#endif // SELFMTIMPL
//eof
