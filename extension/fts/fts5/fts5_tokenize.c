/*
** 2014 May 31
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
** Stripped down to only do Porter stemming for DuckDB
*/

#include <string.h>
#include <assert.h>

#include "fts5_tokenize.h"

static int fts5PorterIsVowel(char c, int bYIsVowel){
  return (
      c=='a' || c=='e' || c=='i' || c=='o' || c=='u' || (bYIsVowel && c=='y')
  );
}

static int fts5PorterGobbleVC(char *zStem, int nStem, int bPrevCons){
  int i;
  int bCons = bPrevCons;

  /* Scan for a vowel */
  for(i=0; i<nStem; i++){
    if( 0==(bCons = !fts5PorterIsVowel(zStem[i], bCons)) ) break;
  }

  /* Scan for a consonent */
  for(i++; i<nStem; i++){
    if( (bCons = !fts5PorterIsVowel(zStem[i], bCons)) ) return i+1;
  }
  return 0;
}

/* porter rule condition: (m > 0) */
static int fts5Porter_MGt0(char *zStem, int nStem){
  return !!fts5PorterGobbleVC(zStem, nStem, 0);
}

/* porter rule condition: (m > 1) */
static int fts5Porter_MGt1(char *zStem, int nStem){
  int n;
  n = fts5PorterGobbleVC(zStem, nStem, 0);
  if( n && fts5PorterGobbleVC(&zStem[n], nStem-n, 1) ){
    return 1;
  }
  return 0;
}

/* porter rule condition: (m = 1) */
static int fts5Porter_MEq1(char *zStem, int nStem){
  int n;
  n = fts5PorterGobbleVC(zStem, nStem, 0);
  if( n && 0==fts5PorterGobbleVC(&zStem[n], nStem-n, 1) ){
    return 1;
  }
  return 0;
}

/* porter rule condition: (*o) */
static int fts5Porter_Ostar(char *zStem, int nStem){
  if( zStem[nStem-1]=='w' || zStem[nStem-1]=='x' || zStem[nStem-1]=='y' ){
    return 0;
  }else{
    int i;
    int mask = 0;
    int bCons = 0;
    for(i=0; i<nStem; i++){
      bCons = !fts5PorterIsVowel(zStem[i], bCons);
      assert( bCons==0 || bCons==1 );
      mask = (mask << 1) + bCons;
    }
    return ((mask & 0x0007)==0x0005);
  }
}

/* porter rule condition: (m > 1 and (*S or *T)) */
static int fts5Porter_MGt1_and_S_or_T(char *zStem, int nStem){
  assert( nStem>0 );
  return (zStem[nStem-1]=='s' || zStem[nStem-1]=='t') 
      && fts5Porter_MGt1(zStem, nStem);
}

/* porter rule condition: (*v*) */
static int fts5Porter_Vowel(char *zStem, int nStem){
  int i;
  for(i=0; i<nStem; i++){
    if( fts5PorterIsVowel(zStem[i], i>0) ){
      return 1;
    }
  }
  return 0;
}


/**************************************************************************
***************************************************************************
** GENERATED CODE STARTS HERE (mkportersteps.tcl)
*/

static int fts5PorterStep4(char *aBuf, int *pnBuf){
  int ret = 0;
  int nBuf = *pnBuf;
  switch( aBuf[nBuf-2] ){
    
    case 'a': 
      if( nBuf>2 && 0==memcmp("al", &aBuf[nBuf-2], 2) ){
        if( fts5Porter_MGt1(aBuf, nBuf-2) ){
          *pnBuf = nBuf - 2;
        }
      }
      break;
  
    case 'c': 
      if( nBuf>4 && 0==memcmp("ance", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt1(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }else if( nBuf>4 && 0==memcmp("ence", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt1(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }
      break;
  
    case 'e': 
      if( nBuf>2 && 0==memcmp("er", &aBuf[nBuf-2], 2) ){
        if( fts5Porter_MGt1(aBuf, nBuf-2) ){
          *pnBuf = nBuf - 2;
        }
      }
      break;
  
    case 'i': 
      if( nBuf>2 && 0==memcmp("ic", &aBuf[nBuf-2], 2) ){
        if( fts5Porter_MGt1(aBuf, nBuf-2) ){
          *pnBuf = nBuf - 2;
        }
      }
      break;
  
    case 'l': 
      if( nBuf>4 && 0==memcmp("able", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt1(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }else if( nBuf>4 && 0==memcmp("ible", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt1(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }
      break;
  
    case 'n': 
      if( nBuf>3 && 0==memcmp("ant", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }else if( nBuf>5 && 0==memcmp("ement", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt1(aBuf, nBuf-5) ){
          *pnBuf = nBuf - 5;
        }
      }else if( nBuf>4 && 0==memcmp("ment", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt1(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }else if( nBuf>3 && 0==memcmp("ent", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 'o': 
      if( nBuf>3 && 0==memcmp("ion", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1_and_S_or_T(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }else if( nBuf>2 && 0==memcmp("ou", &aBuf[nBuf-2], 2) ){
        if( fts5Porter_MGt1(aBuf, nBuf-2) ){
          *pnBuf = nBuf - 2;
        }
      }
      break;
  
    case 's': 
      if( nBuf>3 && 0==memcmp("ism", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 't': 
      if( nBuf>3 && 0==memcmp("ate", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }else if( nBuf>3 && 0==memcmp("iti", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 'u': 
      if( nBuf>3 && 0==memcmp("ous", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 'v': 
      if( nBuf>3 && 0==memcmp("ive", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 'z': 
      if( nBuf>3 && 0==memcmp("ize", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt1(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
  }
  return ret;
}
  

static int fts5PorterStep1B2(char *aBuf, int *pnBuf){
  int ret = 0;
  int nBuf = *pnBuf;
  switch( aBuf[nBuf-2] ){
    
    case 'a': 
      if( nBuf>2 && 0==memcmp("at", &aBuf[nBuf-2], 2) ){
        memcpy(&aBuf[nBuf-2], "ate", 3);
        *pnBuf = nBuf - 2 + 3;
        ret = 1;
      }
      break;
  
    case 'b': 
      if( nBuf>2 && 0==memcmp("bl", &aBuf[nBuf-2], 2) ){
        memcpy(&aBuf[nBuf-2], "ble", 3);
        *pnBuf = nBuf - 2 + 3;
        ret = 1;
      }
      break;
  
    case 'i': 
      if( nBuf>2 && 0==memcmp("iz", &aBuf[nBuf-2], 2) ){
        memcpy(&aBuf[nBuf-2], "ize", 3);
        *pnBuf = nBuf - 2 + 3;
        ret = 1;
      }
      break;
  
  }
  return ret;
}
  

static int fts5PorterStep2(char *aBuf, int *pnBuf){
  int ret = 0;
  int nBuf = *pnBuf;
  switch( aBuf[nBuf-2] ){
    
    case 'a': 
      if( nBuf>7 && 0==memcmp("ational", &aBuf[nBuf-7], 7) ){
        if( fts5Porter_MGt0(aBuf, nBuf-7) ){
          memcpy(&aBuf[nBuf-7], "ate", 3);
          *pnBuf = nBuf - 7 + 3;
        }
      }else if( nBuf>6 && 0==memcmp("tional", &aBuf[nBuf-6], 6) ){
        if( fts5Porter_MGt0(aBuf, nBuf-6) ){
          memcpy(&aBuf[nBuf-6], "tion", 4);
          *pnBuf = nBuf - 6 + 4;
        }
      }
      break;
  
    case 'c': 
      if( nBuf>4 && 0==memcmp("enci", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "ence", 4);
          *pnBuf = nBuf - 4 + 4;
        }
      }else if( nBuf>4 && 0==memcmp("anci", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "ance", 4);
          *pnBuf = nBuf - 4 + 4;
        }
      }
      break;
  
    case 'e': 
      if( nBuf>4 && 0==memcmp("izer", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "ize", 3);
          *pnBuf = nBuf - 4 + 3;
        }
      }
      break;
  
    case 'g': 
      if( nBuf>4 && 0==memcmp("logi", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "log", 3);
          *pnBuf = nBuf - 4 + 3;
        }
      }
      break;
  
    case 'l': 
      if( nBuf>3 && 0==memcmp("bli", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt0(aBuf, nBuf-3) ){
          memcpy(&aBuf[nBuf-3], "ble", 3);
          *pnBuf = nBuf - 3 + 3;
        }
      }else if( nBuf>4 && 0==memcmp("alli", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "al", 2);
          *pnBuf = nBuf - 4 + 2;
        }
      }else if( nBuf>5 && 0==memcmp("entli", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ent", 3);
          *pnBuf = nBuf - 5 + 3;
        }
      }else if( nBuf>3 && 0==memcmp("eli", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt0(aBuf, nBuf-3) ){
          memcpy(&aBuf[nBuf-3], "e", 1);
          *pnBuf = nBuf - 3 + 1;
        }
      }else if( nBuf>5 && 0==memcmp("ousli", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ous", 3);
          *pnBuf = nBuf - 5 + 3;
        }
      }
      break;
  
    case 'o': 
      if( nBuf>7 && 0==memcmp("ization", &aBuf[nBuf-7], 7) ){
        if( fts5Porter_MGt0(aBuf, nBuf-7) ){
          memcpy(&aBuf[nBuf-7], "ize", 3);
          *pnBuf = nBuf - 7 + 3;
        }
      }else if( nBuf>5 && 0==memcmp("ation", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ate", 3);
          *pnBuf = nBuf - 5 + 3;
        }
      }else if( nBuf>4 && 0==memcmp("ator", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "ate", 3);
          *pnBuf = nBuf - 4 + 3;
        }
      }
      break;
  
    case 's': 
      if( nBuf>5 && 0==memcmp("alism", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "al", 2);
          *pnBuf = nBuf - 5 + 2;
        }
      }else if( nBuf>7 && 0==memcmp("iveness", &aBuf[nBuf-7], 7) ){
        if( fts5Porter_MGt0(aBuf, nBuf-7) ){
          memcpy(&aBuf[nBuf-7], "ive", 3);
          *pnBuf = nBuf - 7 + 3;
        }
      }else if( nBuf>7 && 0==memcmp("fulness", &aBuf[nBuf-7], 7) ){
        if( fts5Porter_MGt0(aBuf, nBuf-7) ){
          memcpy(&aBuf[nBuf-7], "ful", 3);
          *pnBuf = nBuf - 7 + 3;
        }
      }else if( nBuf>7 && 0==memcmp("ousness", &aBuf[nBuf-7], 7) ){
        if( fts5Porter_MGt0(aBuf, nBuf-7) ){
          memcpy(&aBuf[nBuf-7], "ous", 3);
          *pnBuf = nBuf - 7 + 3;
        }
      }
      break;
  
    case 't': 
      if( nBuf>5 && 0==memcmp("aliti", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "al", 2);
          *pnBuf = nBuf - 5 + 2;
        }
      }else if( nBuf>5 && 0==memcmp("iviti", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ive", 3);
          *pnBuf = nBuf - 5 + 3;
        }
      }else if( nBuf>6 && 0==memcmp("biliti", &aBuf[nBuf-6], 6) ){
        if( fts5Porter_MGt0(aBuf, nBuf-6) ){
          memcpy(&aBuf[nBuf-6], "ble", 3);
          *pnBuf = nBuf - 6 + 3;
        }
      }
      break;
  
  }
  return ret;
}
  

static int fts5PorterStep3(char *aBuf, int *pnBuf){
  int ret = 0;
  int nBuf = *pnBuf;
  switch( aBuf[nBuf-2] ){
    
    case 'a': 
      if( nBuf>4 && 0==memcmp("ical", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          memcpy(&aBuf[nBuf-4], "ic", 2);
          *pnBuf = nBuf - 4 + 2;
        }
      }
      break;
  
    case 's': 
      if( nBuf>4 && 0==memcmp("ness", &aBuf[nBuf-4], 4) ){
        if( fts5Porter_MGt0(aBuf, nBuf-4) ){
          *pnBuf = nBuf - 4;
        }
      }
      break;
  
    case 't': 
      if( nBuf>5 && 0==memcmp("icate", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ic", 2);
          *pnBuf = nBuf - 5 + 2;
        }
      }else if( nBuf>5 && 0==memcmp("iciti", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "ic", 2);
          *pnBuf = nBuf - 5 + 2;
        }
      }
      break;
  
    case 'u': 
      if( nBuf>3 && 0==memcmp("ful", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt0(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
        }
      }
      break;
  
    case 'v': 
      if( nBuf>5 && 0==memcmp("ative", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          *pnBuf = nBuf - 5;
        }
      }
      break;
  
    case 'z': 
      if( nBuf>5 && 0==memcmp("alize", &aBuf[nBuf-5], 5) ){
        if( fts5Porter_MGt0(aBuf, nBuf-5) ){
          memcpy(&aBuf[nBuf-5], "al", 2);
          *pnBuf = nBuf - 5 + 2;
        }
      }
      break;
  
  }
  return ret;
}
  

static int fts5PorterStep1B(char *aBuf, int *pnBuf){
  int ret = 0;
  int nBuf = *pnBuf;
  switch( aBuf[nBuf-2] ){
    
    case 'e': 
      if( nBuf>3 && 0==memcmp("eed", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_MGt0(aBuf, nBuf-3) ){
          memcpy(&aBuf[nBuf-3], "ee", 2);
          *pnBuf = nBuf - 3 + 2;
        }
      }else if( nBuf>2 && 0==memcmp("ed", &aBuf[nBuf-2], 2) ){
        if( fts5Porter_Vowel(aBuf, nBuf-2) ){
          *pnBuf = nBuf - 2;
          ret = 1;
        }
      }
      break;
  
    case 'n': 
      if( nBuf>3 && 0==memcmp("ing", &aBuf[nBuf-3], 3) ){
        if( fts5Porter_Vowel(aBuf, nBuf-3) ){
          *pnBuf = nBuf - 3;
          ret = 1;
        }
      }
      break;
  
  }
  return ret;
}
  
/* 
** GENERATED CODE ENDS HERE (mkportersteps.tcl)
***************************************************************************
**************************************************************************/

static void fts5PorterStep1A(char *aBuf, int *pnBuf){
  int nBuf = *pnBuf;
  if( aBuf[nBuf-1]=='s' ){
    if( aBuf[nBuf-2]=='e' ){
      if( (nBuf>4 && aBuf[nBuf-4]=='s' && aBuf[nBuf-3]=='s') 
       || (nBuf>3 && aBuf[nBuf-3]=='i' )
      ){
        *pnBuf = nBuf-2;
      }else{
        *pnBuf = nBuf-1;
      }
    }
    else if( aBuf[nBuf-2]!='s' ){
      *pnBuf = nBuf-1;
    }
  }
}

int fts5PorterCb(
  char *aBuf,
  const char *pToken, 
  int nToken
){
  int nBuf = nToken;
  memcpy(aBuf, pToken, nBuf);

  /* Step 1. */
  fts5PorterStep1A(aBuf, &nBuf);
  if( fts5PorterStep1B(aBuf, &nBuf) ){
    if( fts5PorterStep1B2(aBuf, &nBuf)==0 ){
      char c = aBuf[nBuf-1];
      if( fts5PorterIsVowel(c, 0)==0 
       && c!='l' && c!='s' && c!='z' && c==aBuf[nBuf-2] 
      ){
        nBuf--;
      }else if( fts5Porter_MEq1(aBuf, nBuf) && fts5Porter_Ostar(aBuf, nBuf) ){
        aBuf[nBuf++] = 'e';
      }
    }
  }

  /* Step 1C. */
  if( aBuf[nBuf-1]=='y' && fts5Porter_Vowel(aBuf, nBuf-1) ){
    aBuf[nBuf-1] = 'i';
  }

  /* Steps 2 through 4. */
  fts5PorterStep2(aBuf, &nBuf);
  fts5PorterStep3(aBuf, &nBuf);
  fts5PorterStep4(aBuf, &nBuf);

  /* Step 5a. */
  assert( nBuf>0 );
  if( aBuf[nBuf-1]=='e' ){
    if( fts5Porter_MGt1(aBuf, nBuf-1) 
     || (fts5Porter_MEq1(aBuf, nBuf-1) && !fts5Porter_Ostar(aBuf, nBuf-1))
    ){
      nBuf--;
    }
  }

  /* Step 5b. */
  if( nBuf>1 && aBuf[nBuf-1]=='l' 
   && aBuf[nBuf-2]=='l' && fts5Porter_MGt1(aBuf, nBuf-1) 
  ){
    nBuf--;
  }

  return nBuf;
}
