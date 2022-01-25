// MARKER(update_precomp.py): autogen include statement, do not remove

#define _TOOLS_TIME_CXX

#if defined( OS2 )
#define INCL_DOSMISC
#define INCL_DOSDATETIME
#include <svpm.h>
#elif defined( _WIN32 )
#ifdef _MSC_VER
#pragma warning (push,1)
#endif
//#include <tools/svwin.h>
#include "windows.h"
#ifdef _MSC_VER
#pragma warning (pop)
#endif
#elif defined UNX
#include <unistd.h>
#include <limits.h>
#include <math.h>
#include <sys/time.h>
#endif

#include <math.h>
#if !defined(_WIN32)
#include <sys/time.h>
#endif
#include <time.h>
#include <ctime>
#include "define.h"
#include "time.hxx"

#if defined(SOLARIS) && defined(__GNUC__)
extern long altzone;
#endif

namespace duckdb_numformat {

// =======================================================================

static sal_Int32 TimeToSec100( const Time& rTime )
{
	short  nSign   = (rTime.GetTime() >= 0) ? +1 : -1;
	sal_Int32   nHour   = rTime.GetHour();
	sal_Int32   nMin    = rTime.GetMin();
	sal_Int32   nSec    = rTime.GetSec();
	sal_Int32   n100Sec = rTime.Get100Sec();

//	Wegen Interal Compiler Error bei MSC, etwas komplizierter
//	return (n100Sec + (nSec*100) + (nMin*60*100) + (nHour*60*60*100) * nSign);

	sal_Int32 nRet = n100Sec;
	nRet	 += nSec*100;
	nRet	 += nMin*60*100;
	nRet	 += nHour*60*60*100;

	return (nRet * nSign);
}

// -----------------------------------------------------------------------

static Time Sec100ToTime( sal_Int32 nSec100 )
{
	short nSign;
	if ( nSec100 < 0 )
	{
		nSec100 *= -1;
		nSign = -1;
	}
	else
		nSign = 1;

	Time aTime( 0, 0, 0, nSec100 );
	aTime.SetTime( aTime.GetTime() * nSign );
	return aTime;
}

// =======================================================================

Time::Time()
{
#if defined( OS2 )
	PM_DATETIME aDateTime;
	DosGetDateTime( &aDateTime );

	// Zeit zusammenbauen
	nTime = (((sal_Int32)aDateTime.hours)*1000000) +
			(((sal_Int32)aDateTime.minutes)*10000) +
			(((sal_Int32)aDateTime.seconds)*100) +
			((sal_Int32)aDateTime.hundredths);
#else
	time_t	   nTmpTime;
	struct tm aTime;

	// Zeit ermitteln
	nTmpTime = time( 0 );

	// Zeit zusammenbauen
	// if ( localtime_s(&aTime, &nTmpTime ) )
	// {
	// 	nTime = (((sal_Int32)aTime.tm_hour)*1000000) +
	// 			(((sal_Int32)aTime.tm_min)*10000) +
	// 			(((sal_Int32)aTime.tm_sec)*100);
	// }
	// else
		nTime = 0;
#endif
}

// -----------------------------------------------------------------------

Time::Time( const Time& rTime )
{
	nTime = rTime.nTime;
}

// -----------------------------------------------------------------------

Time::Time( sal_uIntPtr nHour, sal_uIntPtr nMin, sal_uIntPtr nSec, sal_uIntPtr n100Sec )
{
	// Zeit normalisieren
	nSec	+= n100Sec / 100;
	n100Sec  = n100Sec % 100;
	nMin	+= nSec / 60;
	nSec	 = nSec % 60;
	nHour	+= nMin / 60;
	nMin	 = nMin % 60;

	// Zeit zusammenbauen
	nTime = (sal_Int32)(n100Sec + (nSec*100) + (nMin*10000) + (nHour*1000000));
}

// -----------------------------------------------------------------------

void Time::SetHour( sal_uInt16 nNewHour )
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	nTime = (n100Sec + (nSec*100) + (nMin*10000) +
			(((sal_Int32)nNewHour)*1000000)) * nSign;
}

// -----------------------------------------------------------------------

void Time::SetMin( sal_uInt16 nNewMin )
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	// kein Ueberlauf
	nNewMin = nNewMin % 60;

	nTime = (n100Sec + (nSec*100) + (((sal_Int32)nNewMin)*10000) +
			(nHour*1000000)) * nSign;
}

// -----------------------------------------------------------------------

void Time::SetSec( sal_uInt16 nNewSec )
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   n100Sec	  = Get100Sec();

	// kein Ueberlauf
	nNewSec = nNewSec % 60;

	nTime = (n100Sec + (((sal_Int32)nNewSec)*100) + (nMin*10000) +
			(nHour*1000000)) * nSign;
}

// -----------------------------------------------------------------------

void Time::Set100Sec( sal_uInt16 nNew100Sec )
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();

	// kein Ueberlauf
	nNew100Sec = nNew100Sec % 100;

	nTime = (((sal_Int32)nNew100Sec) + (nSec*100) + (nMin*10000) +
			(nHour*1000000)) * nSign;
}

// -----------------------------------------------------------------------

sal_Int32 Time::GetMSFromTime() const
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	return (((nHour*3600000)+(nMin*60000)+(nSec*1000)+(n100Sec*10))*nSign);
}

// -----------------------------------------------------------------------

void Time::MakeTimeFromMS( sal_Int32 nMS )
{
	short nSign;
	if ( nMS < 0 )
	{
		nMS *= -1;
		nSign = -1;
	}
	else
		nSign = 1;

	Time aTime( 0, 0, 0, nMS/10 );
	SetTime( aTime.GetTime() * nSign );
}

// -----------------------------------------------------------------------

double Time::GetTimeInDays() const
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
    double nHour      = GetHour();
    double nMin       = GetMin();
    double nSec       = GetSec();
    double n100Sec    = Get100Sec();

    return (nHour+(nMin/60)+(nSec/(60*60))+(n100Sec/(60*60*100))) / 24 * nSign;
}

// -----------------------------------------------------------------------

Time& Time::operator =( const Time& rTime )
{
	nTime = rTime.nTime;
	return *this;
}

// -----------------------------------------------------------------------

Time& Time::operator +=( const Time& rTime )
{
	nTime = Sec100ToTime( TimeToSec100( *this ) +
						  TimeToSec100( rTime ) ).GetTime();
	return *this;
}

// -----------------------------------------------------------------------

Time& Time::operator -=( const Time& rTime )
{
	nTime = Sec100ToTime( TimeToSec100( *this ) -
						  TimeToSec100( rTime ) ).GetTime();
	return *this;
}

// -----------------------------------------------------------------------

Time operator +( const Time& rTime1, const Time& rTime2 )
{
	return Sec100ToTime( TimeToSec100( rTime1 ) +
						 TimeToSec100( rTime2 ) );
}

// -----------------------------------------------------------------------

Time operator -( const Time& rTime1, const Time& rTime2 )
{
	return Sec100ToTime( TimeToSec100( rTime1 ) -
						 TimeToSec100( rTime2 ) );
}

// -----------------------------------------------------------------------

sal_Bool Time::IsEqualIgnore100Sec( const Time& rTime ) const
{
    sal_Int32 n1 = (nTime < 0 ? -Get100Sec() : Get100Sec() );
    sal_Int32 n2 = (rTime.nTime < 0 ? -rTime.Get100Sec() : rTime.Get100Sec() );
    return (nTime - n1) == (rTime.nTime - n2);
}

// -----------------------------------------------------------------------

Time Time::GetUTCOffset()
{
#if defined( OS2 )
#undef timezone
	PM_DATETIME aDateTime;
	DosGetDateTime( &aDateTime );

	// Zeit zusammenbauen
	if ( aDateTime.timezone != -1  )
	{
		short nTempTime = (short)Abs( aDateTime.timezone );
		Time aTime( 0, (sal_uInt16)nTempTime );
		if ( aDateTime.timezone > 0 )
			aTime = -aTime;
		return aTime;
	}
	else
		return Time( 0 );
#elif defined( _WIN32 )
	TIME_ZONE_INFORMATION	aTimeZone;
	aTimeZone.Bias = 0;
	DWORD nTimeZoneRet = GetTimeZoneInformation( &aTimeZone );
	sal_Int32 nTempTime = aTimeZone.Bias;
	if ( nTimeZoneRet == TIME_ZONE_ID_STANDARD )
		nTempTime += aTimeZone.StandardBias;
	else if ( nTimeZoneRet == TIME_ZONE_ID_DAYLIGHT )
		nTempTime += aTimeZone.DaylightBias;
	Time aTime( 0, (sal_uInt16)abs( nTempTime ) );
	if ( nTempTime > 0 )
		aTime = -aTime;
	return aTime;
#else
	static sal_uIntPtr	nCacheTicks = 0;
	static sal_Int32 	nCacheSecOffset = -1;
	sal_uIntPtr			nTicks = Time::GetSystemTicks();
	time_t			nTime;
	tm 			    aTM;
	sal_Int32			nLocalTime;
	sal_Int32			nUTC;
	short			nTempTime;

	// Evt. Wert neu ermitteln
	if ( (nCacheSecOffset == -1)			||
		 ((nTicks - nCacheTicks) > 360000)	||
		 ( nTicks < nCacheTicks ) // handle overflow
		 )
	{
		nTime = time( 0 );
		localtime_r( &nTime, &aTM );
		nLocalTime = mktime( &aTM );
#if defined( SOLARIS )
		// Solaris gmtime_r() seems not to handle daylight saving time
		// flags correctly
		nUTC = nLocalTime + ( aTM.tm_isdst == 0 ? timezone : altzone );
#elif defined( LINUX )
		// Linux mktime() seems not to handle tm_isdst correctly
		nUTC = nLocalTime - aTM.tm_gmtoff;
#else
 		gmtime_r( &nTime, &aTM );
 		nUTC = mktime( &aTM );
#endif
 		nCacheTicks = nTicks;
 		nCacheSecOffset = (nLocalTime-nUTC) / 60;
	}

	nTempTime = (short)abs( nCacheSecOffset );
	Time aTime( 0, (sal_uInt16)nTempTime );
	if ( nCacheSecOffset < 0 )
		aTime = -aTime;
	return aTime;
#endif
}


// -----------------------------------------------------------------------

sal_uIntPtr Time::GetSystemTicks()
{
#if defined _WIN32
	return (sal_uIntPtr)GetTickCount();
#elif defined( OS2 )
	sal_uIntPtr nClock;
	DosQuerySysInfo( QSV_MS_COUNT, QSV_MS_COUNT, &nClock, sizeof( nClock ) );
	return (sal_uIntPtr)nClock;
#else
	timeval tv;
	gettimeofday (&tv, 0);

	double fTicks = tv.tv_sec;
	fTicks *= 1000;
	fTicks += ((tv.tv_usec + 500) / 1000);

	fTicks = fmod (fTicks, double(ULONG_MAX));
	return sal_uIntPtr(fTicks);
#endif
}

// -----------------------------------------------------------------------

sal_uIntPtr Time::GetProcessTicks()
{
#if defined _WIN32
	return (sal_uIntPtr)GetTickCount();
#elif defined( OS2 )
	sal_uIntPtr nClock;
	DosQuerySysInfo( QSV_MS_COUNT, QSV_MS_COUNT, &nClock, sizeof( nClock ) );
	return (sal_uIntPtr)nClock;
#else
	static sal_uIntPtr	nImplTicksPerSecond = 0;
	static double	dImplTicksPerSecond;
	static double	dImplTicksULONGMAX;
	sal_uIntPtr			nTicks = (sal_uIntPtr)clock();

	if ( !nImplTicksPerSecond )
	{
		nImplTicksPerSecond = CLOCKS_PER_SEC;
		dImplTicksPerSecond = nImplTicksPerSecond;
		dImplTicksULONGMAX	= (double)(sal_uIntPtr)ULONG_MAX;
	}

	double fTicks = nTicks;
	fTicks *= 1000;
	fTicks /= dImplTicksPerSecond;
	fTicks = fmod (fTicks, dImplTicksULONGMAX);
	return (sal_uIntPtr)fTicks;
#endif
}
}	// namespace duckdb_numformat
