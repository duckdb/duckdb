/* @(#)bcd2.c	2.1.8.1 */
/*
 * bcd.c: conversion routines for multi-byte arithmetic
 *
 * defined routines:
 * bin_bcd2(long binary, long *low_res, long *high_res)
 * bcd2_bin(long *dest, long bcd)
 * bcd2_add(long *bcd_low, long *bcd_high, long addend)
 * bcd2_sub(long *bcd_low, long *bcd_high, long subend)
 * bcd2_mul(long *bcd_low, long *bcd_high, long multiplier)
 * bcd2_div(long *bcd_low, long *bcd_high, long divisor)
 * long bcd2_mod(long *bcd_low, long *bcd_high, long modulo)
 * long bcd2_cmp(long *bcd_low, long *bcd_high, long compare)
 */
#include <stdio.h>
#include "include/bcd2.h" /* for function prototypes */

#define DIGITS_PER_LONG 7
#define WORD_DIVISOR 10000000
#define GET_DIGIT(num, low, high) \
    ((num) >= DIGITS_PER_LONG) ? (high & (0xF << (4 * ((num) - DIGITS_PER_LONG)))) >> (((num) - DIGITS_PER_LONG) * 4) : (low & (0xF << (4 * (num)))) >> ((num) * 4)
#define SET_DIGIT(value, num, low, high)                            \
    if ((num) >= DIGITS_PER_LONG)                                   \
    {                                                               \
        *high &=                                                    \
            (0xFFFFFFF ^ (0xF << (4 * ((num) - DIGITS_PER_LONG)))); \
        *high |= (value << (4 * ((num) - DIGITS_PER_LONG)));        \
    }                                                               \
    else                                                            \
    {                                                               \
        *low = (*low & (0xFFFFFFF ^ (0xF << (4 * (num)))));         \
        *low |= (value << (4 * (num)));                             \
    }
int bin_bcd2(long binary, long *low_res, long *high_res)
{
    char number[15],
        *current;
    int count;
    long *dest;

    *low_res = *high_res = 0;
    sprintf(number, "%014ld", binary);
    for (current = number, count = 13; *current; current++, count--)
    {
        dest = (count < DIGITS_PER_LONG) ? low_res : high_res;
        *dest = *dest << 4;
        *dest |= *current - '0';
    }
    return (0);
}

int bcd2_bin(long *dest, long bcd)
{
    int count;
    long mask;

    count = DIGITS_PER_LONG - 1;
    mask = 0xF000000;
    *dest = 0;
    while (mask)
    {
        *dest *= 10;
        *dest += (bcd & mask) >> (4 * count);
        mask = mask >> 4;
        count -= 1;
    }
    return (0);
}

int bcd2_add(long *bcd_low, long *bcd_high, long addend)
{
    long tmp_lo, tmp_hi, carry, res;
    int digit;

    bin_bcd2(addend, &tmp_lo, &tmp_hi);
    carry = 0;
    for (digit = 0; digit < 14; digit++)
    {
        res = GET_DIGIT(digit, *bcd_low, *bcd_high);
        res += GET_DIGIT(digit, tmp_lo, tmp_hi);
        res += carry;
        carry = res / 10;
        res %= 10;
        SET_DIGIT(res, digit, bcd_low, bcd_high);
    }
    return (carry);
}

int bcd2_sub(long *bcd_low, long *bcd_high, long subend)
{
    long tmp_lo, tmp_hi, carry, res;
    int digit;

    bin_bcd2(subend, &tmp_lo, &tmp_hi);
    carry = 0;
    for (digit = 0; digit < 14; digit++)
    {
        res = GET_DIGIT(digit, *bcd_low, *bcd_high);
        res -= GET_DIGIT(digit, tmp_lo, tmp_hi);
        res -= carry;
        if (res < 0)
        {
            res += 10;
            carry = 1;
        }
        SET_DIGIT(res, digit, bcd_low, bcd_high);
    }
    return (carry);
}

int bcd2_mul(long *bcd_low, long *bcd_high, long multiplier)
{
    long tmp_lo, tmp_hi, carry, m_lo, m_hi, m1, m2;
    int udigit, ldigit, res;

    tmp_lo = *bcd_low;
    tmp_hi = *bcd_high;
    bin_bcd2(multiplier, &m_lo, &m_hi);
    *bcd_low = 0;
    *bcd_high = 0;
    carry = 0;
    for (ldigit = 0; ldigit < 14; ldigit++)
    {
        m1 = GET_DIGIT(ldigit, m_lo, m_hi);
        carry = 0;
        for (udigit = 0; udigit < 14; udigit++)
        {
            m2 = GET_DIGIT(udigit, tmp_lo, tmp_hi);
            res = m1 * m2;
            res += carry;
            if (udigit + ldigit < 14)
            {
                carry = GET_DIGIT(udigit + ldigit, *bcd_low, *bcd_high);
                res += carry;
            }
            carry = res / 10;
            res %= 10;
            if (udigit + ldigit < 14)
                SET_DIGIT(res, udigit + ldigit, bcd_low, bcd_high);
        }
    }
    return (carry);
}

int bcd2_div(long *bcd_low, long *bcd_high, long divisor)
{
    long tmp_lo, tmp_hi, carry, d1, res, digit;

    carry = 0;
    tmp_lo = *bcd_low;
    tmp_hi = *bcd_high;
    *bcd_low = *bcd_high = 0;
    for (digit = 13; digit >= 0; digit--)
    {
        d1 = GET_DIGIT(digit, tmp_lo, tmp_hi);
        d1 += 10 * carry;
        res = d1 / divisor;
        carry = d1 % divisor;
        SET_DIGIT(res, digit, bcd_low, bcd_high);
    }
    return (carry);
}

long bcd2_mod(long *bcd_low, long *bcd_high, long modulo)
{
    long tmp_low, tmp_high;

    tmp_low = *bcd_low;
    tmp_high = *bcd_high;
    while (tmp_high || tmp_low > modulo)
        bcd2_sub(&tmp_low, &tmp_high, modulo);
    return (tmp_low);
}

long bcd2_cmp(long *low1, long *high1, long comp)
{
    long temp = 0;

    bcd2_bin(&temp, *high1);
    if (temp > 214)
        return (1);
    bcd2_bin(&temp, *low1);
    return (temp - comp);
}

#ifdef TEST_BCD
#include <values.h>

main()
{
    long bin, low_bcd, high_bcd;
    int i;

    bin = MAXINT;
    printf("%ld\n", bin);
    bin_bcd2(bin, &low_bcd, &high_bcd);
    printf("%ld  %ld\n", high_bcd, low_bcd);
    bin = 0;
    bcd2_bin(&bin, high_bcd);
    bcd2_bin(&bin, low_bcd);
    printf("%ld\n", bin);
    for (i = 9; i >= 0; i--)
        printf("%dth digit in %d is %d\n",
               i, bin, GET_DIGIT(i, low_bcd, high_bcd));
    bcd2_add(&low_bcd, &high_bcd, MAXINT);
    bin = 0;
    bcd2_bin(&bin, high_bcd);
    high_bcd = bin;
    bin = 0;
    bcd2_bin(&bin, low_bcd);
    low_bcd = bin;
    printf("%ld%07ld\n", high_bcd, low_bcd);
    bin_bcd2(14, &low_bcd, &high_bcd);
    bcd2_mul(&low_bcd, &high_bcd, 23L);
    bin = 0;
    bcd2_bin(&bin, high_bcd);
    bcd2_bin(&bin, low_bcd);
    printf("%ld\n", bin);
    bcd2_div(&low_bcd, &high_bcd, 10L);
    bin = 0;
    bcd2_bin(&bin, high_bcd);
    bcd2_bin(&bin, low_bcd);
    printf("%ld\n", bin);
}
#endif /* TEST */
