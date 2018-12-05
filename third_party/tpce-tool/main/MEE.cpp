/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Doug Johnson, Matt Emmerton
 */

/******************************************************************************
 *   Description:        Implementation of the MEE class.
 *                       See MEE.h for a description.
 ******************************************************************************/

#include "main/MEE.h"

using namespace TPCE;

// Automatically generate unique RNG seeds.
// The CRandom class uses an unsigned 64-bit value for the seed.
// This routine automatically generates two unique seeds. One is used for
// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
// The 64 bits are used as follows.
//
//  Bits    0 - 31  Caller provided unique unsigned 32-bit id.
//  Bit     32      0 for TxnInputGenerator, 1 for TxnMixGenerator
//  Bits    33 - 43 Number of days since the base time. The base time
//                  is set to be January 1 of the most recent year that is
//                  a multiple of 5. This allows enough space for the last
//                  field, and it makes the algorithm "timeless" by resetting
//                  the generated values every 5 years.
//  Bits    44 - 63 Current time of day measured in 1/10's of a second.
//
void CMEE::AutoSetRNGSeeds(UINT32 UniqueId) {
	CDateTime Now;
	INT32 BaseYear;
	INT32 Tmp1, Tmp2;

	Now.GetYMD(&BaseYear, &Tmp1, &Tmp2);

	// Set the base year to be the most recent year that was a multiple of 5.
	BaseYear -= (BaseYear % 5);
	CDateTime Base(BaseYear, 1, 1); // January 1st in the BaseYear

	// Initialize the seed with the current time of day measured in 1/10's of a
	// second. This will use up to 20 bits.
	RNGSEED Seed;
	Seed = Now.MSec() / 100;

	// Now add in the number of days since the base time.
	// The number of days in the 5 year period requires 11 bits.
	// So shift up by that much to make room in the "lower" bits.
	Seed <<= 11;
	Seed += (RNGSEED)((INT64)Now.DayNo() - (INT64)Base.DayNo());

	// So far, we've used up 31 bits.
	// Save the "last" bit of the "upper" 32 for the RNG id.
	// In addition, make room for the caller's 32-bit unique id.
	// So shift a total of 33 bits.
	Seed <<= 33;

	// Now the "upper" 32-bits have been set with a value for RNG 0.
	// Add in the sponsor's unique id for the "lower" 32-bits.
	Seed += UniqueId;

	// Set the Ticker Tape RNG to the unique seed.
	m_TickerTape.SetRNGSeed(Seed);
	m_DriverMEESettings.cur.TickerTapeRNGSeed = Seed;

	// Set the RNG Id to 1 for the Trading Floor.
	Seed |= UINT64_CONST(0x0000000100000000);
	m_TradingFloor.SetRNGSeed(Seed);
	m_DriverMEESettings.cur.TradingFloorRNGSeed = Seed;
}

// Constructor - automatic RNG seed generation
CMEE::CMEE(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm,
           UINT32 UniqueId)
    : m_DriverMEESettings(UniqueId, 0, 0, 0), m_pSUT(pSUT), m_pLogger(pLogger),
      m_PriceBoard(TradingTimeSoFar, &m_BaseTime, &m_CurrentTime, dfm),
      m_TickerTape(pSUT, &m_PriceBoard, &m_BaseTime, &m_CurrentTime, dfm),
      m_TradingFloor(pSUT, &m_PriceBoard, &m_TickerTape, &m_BaseTime, &m_CurrentTime), m_MEELock() {
	m_pLogger->SendToLogger("MEE object constructed using c'tor 1 (valid for publication: YES).");

	AutoSetRNGSeeds(UniqueId);

	m_pLogger->SendToLogger(m_DriverMEESettings);
}

// Constructor - RNG seed provided
CMEE::CMEE(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm,
           UINT32 UniqueId, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed)
    : m_DriverMEESettings(UniqueId, 0, TickerTapeRNGSeed, TradingFloorRNGSeed), m_pSUT(pSUT), m_pLogger(pLogger),
      m_PriceBoard(TradingTimeSoFar, &m_BaseTime, &m_CurrentTime, dfm),
      m_TickerTape(pSUT, &m_PriceBoard, &m_BaseTime, &m_CurrentTime, TickerTapeRNGSeed, dfm),
      m_TradingFloor(pSUT, &m_PriceBoard, &m_TickerTape, &m_BaseTime, &m_CurrentTime, TradingFloorRNGSeed),
      m_MEELock() {
	m_pLogger->SendToLogger("MEE object constructed using c'tor 2 (valid for publication: NO).");
	m_pLogger->SendToLogger(m_DriverMEESettings);
}

CMEE::~CMEE(void) {
	m_pLogger->SendToLogger("MEE object destroyed.");
}

RNGSEED CMEE::GetTickerTapeRNGSeed(void) {
	return (m_TickerTape.GetRNGSeed());
}

RNGSEED CMEE::GetTradingFloorRNGSeed(void) {
	return (m_TradingFloor.GetRNGSeed());
}

void CMEE::SetBaseTime(void) {
	m_MEELock.lock();
	m_BaseTime.Set();
	m_MEELock.unlock();
}

bool CMEE::DisableTickerTape(void) {
	bool Result;
	m_MEELock.lock();
	Result = m_TickerTape.DisableTicker();
	m_MEELock.unlock();
	return (Result);
}

bool CMEE::EnableTickerTape(void) {
	bool Result;
	m_MEELock.lock();
	Result = m_TickerTape.EnableTicker();
	m_MEELock.unlock();
	return (Result);
}

INT32 CMEE::GenerateTradeResult(void) {
	INT32 NextTime;

	m_MEELock.lock();
	m_CurrentTime.Set();
	NextTime = m_TradingFloor.GenerateTradeResult();
	m_MEELock.unlock();
	return (NextTime);
}

INT32 CMEE::SubmitTradeRequest(PTradeRequest pTradeRequest) {
	INT32 NextTime;

	m_MEELock.lock();
	m_CurrentTime.Set();
	NextTime = m_TradingFloor.SubmitTradeRequest(pTradeRequest);
	m_MEELock.unlock();
	return (NextTime);
}
