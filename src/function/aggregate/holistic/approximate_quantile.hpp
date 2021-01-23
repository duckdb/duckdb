/* Copyright 2016 Jason Ge*/
/* Implementation of the paper */
/* A Fast Algorithm for Approximate Quantiles in High Speed Data Streams*/
/* for answering quantile query with error bounded by eps.*/
/* If we're interested in quantile with precision up to 1%, */
/* eps can be set as 0.01. */
/* Space complexity is O(log(eps*n)^2/eps), growing as O(log(n)^2).*/
/* Time complexity for update operation (run time complexity) is */
/* O(log(log(eps*n)/eps)), which is almost negligible.*/

/* The implementation supports stream of data of any class T, */
/* as long as the < operator is defined. */

#include <math.h>
#include <vector>
#include <iostream>
#include <exception>
#include <algorithm>
#include <stdexcept>

template<class T>
class CApproxRank{
 public:
  T v;
  int rmin;
  int rmax;

  CApproxRank() {
    rmin = 0;
    rmax = 0;
  };

  CApproxRank(T ev, int ermin, int ermax) {
    v = ev;
    rmin = ermin;
    rmax = ermax;
  }

  bool operator<(const CApproxRank<T> & r) const {return(v < r.v);}
};

template <class T>
class CSummary {
 private:
    // errow rate
    double epsilon;

    // block size
    int b;

    // maximal number of levels
    int L;

    // current count of elements
    double cnt;

    std::vector<std::vector<CApproxRank<T>>> S;

 public:
    std::vector<CApproxRank<T> > merge(const std::vector<CApproxRank<T> > & Sa,
                                const std::vector<CApproxRank<T> > & Sb);

    std::vector<CApproxRank<T> > compress(const std::vector<CApproxRank<T> > & S0, int B);

 public:
    CSummary();

    void update(T e);

    bool init(int levelN, double eps);

    // merge all levels in the summary and compress to
    // epsilon-approximate summary
    std::vector<CApproxRank<T> > compress_all(double epsilon);

    double query(T e);

    int size() {return(cnt);}
};

template<class T>
std::vector<CApproxRank<T> > CSummary<T>::merge(const std::vector<CApproxRank<T> > & Sa, const std::vector<CApproxRank<T> > & Sb) {
  if (Sa.size() == 0) {
    std::vector<CApproxRank<T> > Sm(Sb);
    return(Sm);
  }

  if (Sb.size() == 0) {
    std::vector<CApproxRank<T> > Sm(Sa);
    return(Sm);
  }

  std::vector<CApproxRank<T> > Sm;
  Sm.clear();

  int i1 = 0;
  int i2 = 0;
  int from = 0;
  while (i1 < Sa.size() || i2 < Sb.size()) {
    CApproxRank<T> approx_rank;

    if (i1 < Sa.size() && i2 < Sb.size()) {
      if (Sa[i1].v < Sb[i2].v) {
        approx_rank.v = Sa[i1].v;
        from = 1;
      } else {
        approx_rank.v = Sb[i2].v;
        from = 2;
      }
    } else if (i1 < Sa.size() && i2 >= Sb.size()) {
      approx_rank.v = Sa[i1].v;
      from = 1;
    } else if (i1 >= Sa.size() && i2 < Sb.size()) {
      approx_rank.v = Sb[i2].v;
      from = 2;
    }

    if (from == 1) {
      if (0 < i2 && i2 < Sb.size()) {
        approx_rank.rmin = Sa[i1].rmin + Sb[i2-1].rmin;
        approx_rank.rmax = Sa[i1].rmax + Sb[i2].rmax - 1;
      } else if (i2 == 0){
        approx_rank.rmin = Sa[i1].rmin;
        approx_rank.rmax = Sa[i1].rmax + Sb[i2].rmax - 1;
      } else if (i2 == Sb.size()) {
        approx_rank.rmin = Sa[i1].rmin + Sb[i2-1].rmin;
        approx_rank.rmax = Sa[i1].rmax + Sb[i2-1].rmax;
      }
      i1 ++;
    }

    if (from == 2) {
      if (0 < i1 && i1 < Sa.size()) {
        approx_rank.rmin = Sa[i1-1].rmin + Sb[i2].rmin;
        approx_rank.rmax = Sa[i1].rmax + Sb[i2].rmax -1;
      } else if (i1 == 0) {
        approx_rank.rmin = Sb[i2].rmin;
        approx_rank.rmax = Sa[i1].rmax + Sb[i2].rmax - 1;
      } else if (i1 == Sa.size()) {
        approx_rank.rmin = Sa[i1-1].rmin + Sb[i2].rmin;
        approx_rank.rmax = Sa[i1-1].rmax + Sb[i2].rmax;
      }
      i2++;
    }

    Sm.push_back(approx_rank);
  }

  return(Sm);
}

template<class T>
std::vector<CApproxRank<T> > CSummary<T>::compress(const std::vector<CApproxRank<T> > & S0, int B) {
  std::vector<CApproxRank<T> > Sc;
  Sc.clear();

  int i = 0; // i = 0..B
  int j = 0; // j = 0..|S0|

  int S0_range = 0;
  double e = 0; // maximum error in the vector
  for (int i = 0; i < S0.size(); i++) {
    if (S0_range < S0[i].rmax) {
      S0_range = S0[i].rmax;
    }
    if (S0[i].rmax - S0[i].rmin > e) {
      e = S0[i].rmax - S0[i].rmin;
    }
  }

  double epsN = epsilon * S0_range;
  if (2 * epsN < e) {
    throw std::domain_error("The precision property of the algorithm is found to be violated. Stop compression.");
  }

  while (i<=B && j<S0.size()) {
    int r = static_cast<int>(floor(static_cast<double>(i * S0_range) / B));

    while (j<S0.size()){
  //    if (r - S0[j].rmin <=epsN && S0[j].rmax-r <=epsN) {
      if (S0[j].rmax >= r) {
        break;
      }
      j++;
    }

    if (j<S0.size()) {
      Sc.push_back(S0[j]);
      j++;
    } else {
      throw std::domain_error("Compression error: cannot find the summary with precision epsilon*N.");
    }

    i++;
  }

  return(Sc);
}

template<class T>
std::vector<CApproxRank<T> > CSummary<T>::compress_all(double eps) {
  std::vector<CApproxRank<T> > Sm;
  return(Sm);
}

template<class T>
CSummary<T>::CSummary() {
  L = 0;
  cnt = 0;
  b = 0;
  epsilon = 0;
}

template<class T>
bool CSummary<T>::init(int levelN, double eps) {
  // Initialize the class with the number of levels,
  // and the error rate.
  L = levelN;

  try {
    S.clear();
    S.resize(L);
  } catch (std::exception & e) {
    std::cout << e.what() << std::endl;
    return(false);
  }

  epsilon = eps;

  if (fabs(eps) < 1e-8) {
    return(false);
  }

  /* N = 2^L, b = floor(log(eps*N)/eps) */
  b = static_cast<int>(floor(L/epsilon + log(epsilon)/(epsilon*log(2))));
  return(true);
}

template<class T>
void CSummary<T>::update(T e) {
  CApproxRank<T> approx_rank(e, 0, 0);

  typename std::vector<CApproxRank<T> >::iterator idx = std::upper_bound(S[0].begin(), S[0].end(), approx_rank);
  approx_rank.rmin = (idx - S[0].begin());
  approx_rank.rmax = approx_rank.rmin;

  S[0].insert(idx, approx_rank);


 // int i = static_cast<int>(idx - S[0].begin());
  //S[0][i].rmin = i;
  //S[0][i].rmax= i;
  cnt++;

  if (S[0].size() < b) {
    return;
  }

  /* level 0 is full. Pack it up */
  //std::sort(S[0].begin(), S[0].end());
  for (int i = 0; i < S[0].size(); i++){
    S[0][i].rmin = i;
    S[0][i].rmax = i;
  }

  int compressed_size = b/2;
  std::vector<CApproxRank<T> > Sc = compress(S[0], compressed_size);

  S[0].clear();

  for (int k=1; k < L; k++) {
    if (S[k].size() == 0) {
      S[k] = Sc;
      break;
    } else {
      std::vector<CApproxRank<T> > tmp = merge(S[k], Sc);
      Sc = compress(tmp, compressed_size);
      S[k].clear();
    }
  }
}

template<class T>
double CSummary<T>::query(T e) {
  //std::sort(S[0].begin(), S[0].end());
  for (int i = 0; i < S[0].size(); i++){
    S[0][i].rmin = i;
    S[0][i].rmax = i;
  }

  std::vector<CApproxRank<T> > Sm(S[0]);

  for (int i =1; i<L; i++) {
    Sm = merge(Sm, S[i]);
  }

  int i = 0;
  while (i < Sm.size()){
    if (Sm[i].v >= e) break;
    i++;
  }

  double q = static_cast<double>(Sm[i].rmin+Sm[i].rmax)/(2*cnt);
  if (q<0) q = 0;
  return(q);
}
