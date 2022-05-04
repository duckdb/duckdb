// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
 **********************************************************************
 * Copyright (c) 2003-2008, International Business Machines
 * Corporation and others.  All Rights Reserved.
 **********************************************************************
 * Author: Alan Liu
 * Created: September 2 2003
 * Since: ICU 2.8
 **********************************************************************
 */

#include "gregoimp.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/ucal.h"
#include "uresimp.h"
#include "cstring.h"
#include "uassert.h"

U_NAMESPACE_BEGIN

int32_t ClockMath::floorDivide(int32_t numerator, int32_t denominator) {
    return (numerator >= 0) ?
        numerator / denominator : ((numerator + 1) / denominator) - 1;
}

int64_t ClockMath::floorDivide(int64_t numerator, int64_t denominator) {
    return (numerator >= 0) ?
        numerator / denominator : ((numerator + 1) / denominator) - 1;
}

int32_t ClockMath::floorDivide(double numerator, int32_t denominator,
                          int32_t& remainder) {
    double quotient;
    quotient = uprv_floor(numerator / denominator);
    remainder = (int32_t) (numerator - (quotient * denominator));
    return (int32_t) quotient;
}

double ClockMath::floorDivide(double dividend, double divisor,
                         double& remainder) {
    // Only designed to work for positive divisors
    U_ASSERT(divisor > 0);
    double quotient = floorDivide(dividend, divisor);
    remainder = dividend - (quotient * divisor);
    // N.B. For certain large dividends, on certain platforms, there
    // is a bug such that the quotient is off by one.  If you doubt
    // this to be true, set a breakpoint below and run cintltst.
    if (remainder < 0 || remainder >= divisor) {
        // E.g. 6.7317038241449352e+022 / 86400000.0 is wrong on my
        // machine (too high by one).  4.1792057231752762e+024 /
        // 86400000.0 is wrong the other way (too low).
        double q = quotient;
        quotient += (remainder < 0) ? -1 : +1;
        if (q == quotient) {
            // For quotients > ~2^53, we won't be able to add or
            // subtract one, since the LSB of the mantissa will be >
            // 2^0; that is, the exponent (base 2) will be larger than
            // the length, in bits, of the mantissa.  In that case, we
            // can't give a correct answer, so we set the remainder to
            // zero.  This has the desired effect of making extreme
            // values give back an approximate answer rather than
            // crashing.  For example, UDate values above a ~10^25
            // might all have a time of midnight.
            remainder = 0;
        } else {
            remainder = dividend - (quotient * divisor);
        }
    }
    U_ASSERT(0 <= remainder && remainder < divisor);
    return quotient;
}

const int32_t JULIAN_1_CE    = 1721426; // January 1, 1 CE Gregorian
const int32_t JULIAN_1970_CE = 2440588; // January 1, 1970 CE Gregorian

const int16_t Grego::DAYS_BEFORE[24] =
    {0,31,59,90,120,151,181,212,243,273,304,334,
     0,31,60,91,121,152,182,213,244,274,305,335};

const int8_t Grego::MONTH_LENGTH[24] =
    {31,28,31,30,31,30,31,31,30,31,30,31,
     31,29,31,30,31,30,31,31,30,31,30,31};

double Grego::fieldsToDay(int32_t year, int32_t month, int32_t dom) {

    int32_t y = year - 1;

    double julian = 365 * y + ClockMath::floorDivide(y, 4) + (JULIAN_1_CE - 3) + // Julian cal
        ClockMath::floorDivide(y, 400) - ClockMath::floorDivide(y, 100) + 2 + // => Gregorian cal
        DAYS_BEFORE[month + (isLeapYear(year) ? 12 : 0)] + dom; // => month/dom

    return julian - JULIAN_1970_CE; // JD => epoch day
}

void Grego::dayToFields(double day, int32_t& year, int32_t& month,
                        int32_t& dom, int32_t& dow, int32_t& doy) {

    // Convert from 1970 CE epoch to 1 CE epoch (Gregorian calendar)
    day += JULIAN_1970_CE - JULIAN_1_CE;

    // Convert from the day number to the multiple radix
    // representation.  We use 400-year, 100-year, and 4-year cycles.
    // For example, the 4-year cycle has 4 years + 1 leap day; giving
    // 1461 == 365*4 + 1 days.
    int32_t n400 = ClockMath::floorDivide(day, 146097, doy); // 400-year cycle length
    int32_t n100 = ClockMath::floorDivide(doy, 36524, doy); // 100-year cycle length
    int32_t n4   = ClockMath::floorDivide(doy, 1461, doy); // 4-year cycle length
    int32_t n1   = ClockMath::floorDivide(doy, 365, doy);
    year = 400*n400 + 100*n100 + 4*n4 + n1;
    if (n100 == 4 || n1 == 4) {
        doy = 365; // Dec 31 at end of 4- or 400-year cycle
    } else {
        ++year;
    }
    
    UBool isLeap = isLeapYear(year);
    
    // Gregorian day zero is a Monday.
    dow = (int32_t) uprv_fmod(day + 1, 7);
    dow += (dow < 0) ? (UCAL_SUNDAY + 7) : UCAL_SUNDAY;

    // Common Julian/Gregorian calculation
    int32_t correction = 0;
    int32_t march1 = isLeap ? 60 : 59; // zero-based DOY for March 1
    if (doy >= march1) {
        correction = isLeap ? 1 : 2;
    }
    month = (12 * (doy + correction) + 6) / 367; // zero-based month
    dom = doy - DAYS_BEFORE[month + (isLeap ? 12 : 0)] + 1; // one-based DOM
    doy++; // one-based doy
}

void Grego::timeToFields(UDate time, int32_t& year, int32_t& month,
                        int32_t& dom, int32_t& dow, int32_t& doy, int32_t& mid) {
    double millisInDay;
    double day = ClockMath::floorDivide((double)time, (double)U_MILLIS_PER_DAY, millisInDay);
    mid = (int32_t)millisInDay;
    dayToFields(day, year, month, dom, dow, doy);
}

int32_t Grego::dayOfWeek(double day) {
    int32_t dow;
    ClockMath::floorDivide(day + UCAL_THURSDAY, 7, dow);
    return (dow == 0) ? UCAL_SATURDAY : dow;
}

int32_t Grego::dayOfWeekInMonth(int32_t year, int32_t month, int32_t dom) {
    int32_t weekInMonth = (dom + 6)/7;
    if (weekInMonth == 4) {
        if (dom + 7 > monthLength(year, month)) {
            weekInMonth = -1;
        }
    } else if (weekInMonth == 5) {
        weekInMonth = -1;
    }
    return weekInMonth;
}

U_NAMESPACE_END

#endif
//eof
