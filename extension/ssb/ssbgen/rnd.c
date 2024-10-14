/* @(#)rnd.c	2.1.8.2
 *
 *
 * RANDOM.C -- Implements Park & Miller's "Minimum Standard" RNG
 *
 * (Reference:  CACM, Oct 1988, pp 1192-1201)
 *
 * NextRand:  Computes next random integer
 * UnifInt:   Yields an long uniformly distributed between given bounds
 * UnifReal: ields a real uniformly distributed between given bounds
 * Exponential: Yields a real exponentially distributed with given mean
 *
 */

#include "include/config.h"
#include <stdio.h>
#include <math.h>
#include "include/dss.h"
#include "include/rnd.h"

char *env_config PROTO((char *tag, char *dflt));
void NthElement(long, long *);

void dss_random(long *tgt, long lower, long upper, long stream)
{
    *tgt = UnifInt((long)lower, (long)upper, (long)stream);
    Seed[stream].usage += 1;

    return;
}

void row_start(int t)
{
    int i;
    for (i = 0; i <= MAX_STREAM; i++)
        Seed[i].usage = 0;

    return;
}

void row_stop(int t)
{
    int i;

    /* need to allow for handling the master and detail together */
    if (t == ORDER_LINE)
        t = ORDER;
    if (t == PART_PSUPP)
        t = PART;

    for (i = 0; i <= MAX_STREAM; i++)
        if ((Seed[i].table == t) || (Seed[i].table == tdefs[t].child))
        {
            if (set_seeds && (Seed[i].usage > Seed[i].boundary))
            {
                fprintf(stderr, "\nSEED CHANGE: seed[%d].usage = %d\n",
                        i, Seed[i].usage);
                Seed[i].boundary = Seed[i].usage;
            }
            else
            {
                NthElement((Seed[i].boundary - Seed[i].usage), &Seed[i].value);
            }
        }
    return;
}

void dump_seeds(int tbl)
{
    int i;

    for (i = 0; i <= MAX_STREAM; i++)
        if (Seed[i].table == tbl)
            printf("%d:\t%ld\n", i, Seed[i].value);
    return;
}

/******************************************************************

   NextRand:  Computes next random integer

*******************************************************************/

/*
 * long NextRand( long nSeed )
 */
long NextRand(long nSeed)

/*
 * nSeed is the previous random number; the returned value is the
 * next random number. The routine generates all numbers in the
 * range 1 .. nM-1.
 */

{

    /*
     * The routine returns (nSeed * nA) mod nM, where   nA (the
     * multiplier) is 16807, and nM (the modulus) is
     * 2147483647 = 2^31 - 1.
     *
     * nM is prime and nA is a primitive element of the range 1..nM-1.
     * This * means that the map nSeed = (nSeed*nA) mod nM, starting
     * from any nSeed in 1..nM-1, runs through all elements of 1..nM-1
     * before repeating.  It never hits 0 or nM.
     *
     * To compute (nSeed * nA) mod nM without overflow, use the
     * following trick.  Write nM as nQ * nA + nR, where nQ = nM / nA
     * and nR = nM % nA.   (For nM = 2147483647 and nA = 16807,
     * get nQ = 127773 and nR = 2836.) Write nSeed as nU * nQ + nV,
     * where nU = nSeed / nQ and nV = nSeed % nQ.  Then we have:
     *
     * nM  =  nA * nQ  +  nR        nQ = nM / nA        nR < nA < nQ
     *
     * nSeed = nU * nQ  +  nV       nU = nSeed / nQ     nV < nU
     *
     * Since nA < nQ, we have nA*nQ < nM < nA*nQ + nA < nA*nQ + nQ,
     * i.e., nM/nQ = nA.  This gives bounds on nU and nV as well:
     * nM > nSeed  =>  nM/nQ * >= nSeed/nQ  =>  nA >= nU ( > nV ).
     *
     * Using ~ to mean "congruent mod nM" this gives:
     *
     * nA * nSeed  ~  nA * (nU*nQ + nV)
     *
     * ~  nA*nU*nQ + nA*nV
     *
     * ~  nU * (-nR)  +  nA*nV      (as nA*nQ ~ -nR)
     *
     * Both products in the last sum can be computed without overflow
     * (i.e., both have absolute value < nM) since nU*nR < nA*nQ < nM,
     * and  nA*nV < nA*nQ < nM.  Since the two products have opposite
     * sign, their sum lies between -(nM-1) and +(nM-1).  If
     * non-negative, it is the answer (i.e., it's congruent to
     * nA*nSeed and lies between 0 and nM-1). Otherwise adding nM
     * yields a number still congruent to nA*nSeed, but now between
     * 0 and nM-1, so that's the answer.
     */

    long nU, nV;

    nU = nSeed / nQ;
    nV = nSeed - nQ * nU; /* i.e., nV = nSeed % nQ */
    nSeed = nA * nV - nU * nR;
    if (nSeed < 0)
        nSeed += nM;
    return (nSeed);
}

/******************************************************************

   UnifInt:  Yields an long uniformly distributed between given bounds

*******************************************************************/

/*
 * long UnifInt( long nLow, long nHigh, long nStream )
 */
long UnifInt(long nLow, long nHigh, long nStream)

/*
 * Returns an integer uniformly distributed between nLow and nHigh,
 * including * the endpoints.  nStream is the random number stream.
 * Stream 0 is used if nStream is not in the range 0..MAX_STREAM.
 */

{
    double dRange;
    long nTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;

    if (nLow > nHigh)
    {
        nTemp = nLow;
        nLow = nHigh;
        nHigh = nTemp;
    }

    dRange = DOUBLE_CAST(nHigh - nLow + 1);
    Seed[nStream].value = NextRand(Seed[nStream].value);
    nTemp = (long)(((double)Seed[nStream].value / dM) * (dRange));
    return (nLow + nTemp);
}

/******************************************************************

   UnifReal:  Yields a real uniformly distributed between given bounds

*******************************************************************/

/*
 * double UnifReal( double dLow, double dHigh, long nStream )
 */
double
UnifReal(double dLow, double dHigh, long nStream)

/*
 * Returns a double uniformly distributed between dLow and dHigh,
 * excluding the endpoints.  nStream is the random number stream.
 * Stream 0 is used if nStream is not in the range 0..MAX_STREAM.
 */

{
    double dTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
    if (dLow == dHigh)
        return (dLow);
    if (dLow > dHigh)
    {
        dTemp = dLow;
        dLow = dHigh;
        dHigh = dTemp;
    }
    Seed[nStream].value = NextRand(Seed[nStream].value);
    dTemp = ((double)Seed[nStream].value / dM) * (dHigh - dLow);
    return (dLow + dTemp);
}

/******************************************************************%

   Exponential:  Yields a real exponentially distributed with given mean

*******************************************************************/

/*
 * double Exponential( double dMean, long nStream )
 */
double
Exponential(double dMean, long nStream)

/*
 * Returns a double uniformly distributed with mean dMean.
 * 0.0 is returned iff dMean <= 0.0. nStream is the random number
 * stream. Stream 0 is used if nStream is not in the range
 * 0..MAX_STREAM.
 */

{
    double dTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
    if (dMean <= 0.0)
        return (0.0);

    Seed[nStream].value = NextRand(Seed[nStream].value);
    dTemp = (double)Seed[nStream].value / dM; /* unif between 0..1 */
    return (-dMean * log(1.0 - dTemp));
}
