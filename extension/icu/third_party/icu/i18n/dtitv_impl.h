// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
* Copyright (C) 2007-2016, International Business Machines Corporation and
* others. All Rights Reserved.
*******************************************************************************
*
* File DTITV_IMPL.H
*
*******************************************************************************
*/


#ifndef DTITV_IMPL_H__
#define DTITV_IMPL_H__

/**
 * \file
 * \brief C++ API: Defines macros for interval format implementation
 */

#if !UCONFIG_NO_FORMATTING

#include "unicode/unistr.h"


#define dtitv_impl_QUOTE             ((UChar)0x0027)
#define dtitv_impl_LOW_LINE          ((UChar)0x005F)
#define dtitv_impl_COLON             ((UChar)0x003A)
#define dtitv_impl_LEFT_CURLY_BRACKET  ((UChar)0x007B)
#define dtitv_impl_RIGHT_CURLY_BRACKET ((UChar)0x007D)
#define dtitv_impl_SPACE             ((UChar)0x0020)
#define dtitv_impl_EN_DASH           ((UChar)0x2013)
#define dtitv_impl_SOLIDUS           ((UChar)0x002F)

#define dtitv_impl_DIGIT_ZERO        ((UChar)0x0030)
#define dtitv_impl_DIGIT_ONE         ((UChar)0x0031)

#define dtitv_impl_LOW_A             ((UChar)0x0061)
#define dtitv_impl_LOW_B             ((UChar)0x0062)
#define dtitv_impl_LOW_C             ((UChar)0x0063)
#define dtitv_impl_LOW_D             ((UChar)0x0064)
#define dtitv_impl_LOW_E             ((UChar)0x0065)
#define dtitv_impl_LOW_F             ((UChar)0x0066)
#define dtitv_impl_LOW_G             ((UChar)0x0067)
#define dtitv_impl_LOW_H             ((UChar)0x0068)
#define dtitv_impl_LOW_I             ((UChar)0x0069)
#define dtitv_impl_LOW_J             ((UChar)0x006a)
#define dtitv_impl_LOW_K             ((UChar)0x006B)
#define dtitv_impl_LOW_L             ((UChar)0x006C)
#define dtitv_impl_LOW_M             ((UChar)0x006D)
#define dtitv_impl_LOW_N             ((UChar)0x006E)
#define dtitv_impl_LOW_O             ((UChar)0x006F)
#define dtitv_impl_LOW_P             ((UChar)0x0070)
#define dtitv_impl_LOW_Q             ((UChar)0x0071)
#define dtitv_impl_LOW_R             ((UChar)0x0072)
#define dtitv_impl_LOW_S             ((UChar)0x0073)
#define dtitv_impl_LOW_T             ((UChar)0x0074)
#define dtitv_impl_LOW_U             ((UChar)0x0075)
#define dtitv_impl_LOW_V             ((UChar)0x0076)
#define dtitv_impl_LOW_W             ((UChar)0x0077)
#define dtitv_impl_LOW_Y             ((UChar)0x0079)
#define dtitv_impl_LOW_Z             ((UChar)0x007A)

#define dtitv_impl_CAP_A             ((UChar)0x0041)
#define dtitv_impl_CAP_C             ((UChar)0x0043)
#define dtitv_impl_CAP_D             ((UChar)0x0044)
#define dtitv_impl_CAP_E             ((UChar)0x0045)
#define dtitv_impl_CAP_F             ((UChar)0x0046)
#define dtitv_impl_CAP_G             ((UChar)0x0047)
#define dtitv_impl_CAP_H             ((UChar)0x0048)
#define dtitv_impl_CAP_K             ((UChar)0x004B)
#define dtitv_impl_CAP_L             ((UChar)0x004C)
#define dtitv_impl_CAP_M             ((UChar)0x004D)
#define dtitv_impl_CAP_O             ((UChar)0x004F)
#define dtitv_impl_CAP_Q             ((UChar)0x0051)
#define dtitv_impl_CAP_S             ((UChar)0x0053)
#define dtitv_impl_CAP_T             ((UChar)0x0054)
#define dtitv_impl_CAP_U             ((UChar)0x0055)
#define dtitv_impl_CAP_V             ((UChar)0x0056)
#define dtitv_impl_CAP_W             ((UChar)0x0057)
#define dtitv_impl_CAP_Y             ((UChar)0x0059)
#define dtitv_impl_CAP_Z             ((UChar)0x005A)

//#define MINIMUM_SUPPORTED_CALENDAR_FIELD    UCAL_MINUTE

#define dtitv_impl_MAX_E_COUNT      5
#define dtitv_impl_MAX_M_COUNT      5
//#define MAX_INTERVAL_INDEX 4
#define dtitv_impl_MAX_POSITIVE_INT  56632


#endif /* #if !UCONFIG_NO_FORMATTING */

#endif
//eof
