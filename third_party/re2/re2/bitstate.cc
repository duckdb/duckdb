// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tested by search_test.cc, exhaustive_test.cc, tester.cc

// Prog::SearchBitState is a regular expression search with submatch
// tracking for small regular expressions and texts.  Similarly to
// testing/backtrack.cc, it allocates a bitmap with (count of
// lists) * (length of text) bits to make sure it never explores the
// same (instruction list, character position) multiple times.  This
// limits the search to run in time linear in the length of the text.
//
// Unlike testing/backtrack.cc, SearchBitState is not recursive
// on the text.
//
// SearchBitState is a fast replacement for the NFA code on small
// regexps and texts when SearchOnePass cannot be used.

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <limits>
#include <utility>

#include "util/logging.h"
#include "re2/pod_array.h"
#include "re2/prog.h"
#include "re2/regexp.h"

namespace duckdb_re2 {

struct Job {
  int id;
  int rle;  // run length encoding
  const char* p;
};

class BitState {
 public:
  explicit BitState(Prog* prog);

  // The usual Search prototype.
  // Can only call Search once per BitState.
  bool Search(const StringPiece& text, const StringPiece& context,
              bool anchored, bool longest,
              StringPiece* submatch, int nsubmatch);

 private:
  inline bool ShouldVisit(int id, const char* p);
  inline void Push(int id, const char* p);
  inline void PushRun(int id, const char* p, size_t n);
  size_t VisitRun(int list, const char* pfirst, size_t m);
  size_t FirstVisited(int list, const char* pfirst, size_t m);
  void SetVisited(int list, const char* pfirst, size_t m);
  void GrowStack();
  bool TrySearch(int id, const char* p);

  // Search parameters
  Prog* prog_;              // program being run
  StringPiece text_;        // text being searched
  StringPiece context_;     // greater context of text being searched
  bool anchored_;           // whether search is anchored at text.begin()
  bool longest_;            // whether search wants leftmost-longest match
  bool endmatch_;           // whether match must end at text.end()
  StringPiece* submatch_;   // submatches to fill in
  int nsubmatch_;           //   # of submatches to fill in

  // Search state
  static constexpr int kVisitedBits = 64;
  PODArray<uint64_t> visited_;  // bitmap: (list ID, char*) pairs visited
  PODArray<const char*> cap_;   // capture registers
  PODArray<Job> job_;           // stack of text positions to explore
  int njob_;                    // stack size

  BitState(const BitState&) = delete;
  BitState& operator=(const BitState&) = delete;
};

BitState::BitState(Prog* prog)
  : prog_(prog),
    anchored_(false),
    longest_(false),
    endmatch_(false),
    submatch_(NULL),
    nsubmatch_(0),
    njob_(0) {
}

// Given id, which *must* be a list head, we can look up its list ID.
// Then the question is: Should the search visit the (list ID, p) pair?
// If so, remember that it was visited so that the next time,
// we don't repeat the visit.
bool BitState::ShouldVisit(int id, const char* p) {
  int n = prog_->list_heads()[id] * static_cast<int>(text_.size()+1) +
          static_cast<int>(p-text_.data());
  if (visited_[n/kVisitedBits] & (uint64_t{1} << (n & (kVisitedBits-1))))
    return false;
  visited_[n/kVisitedBits] |= uint64_t{1} << (n & (kVisitedBits-1));
  return true;
}

// Grow the stack.
void BitState::GrowStack() {
  PODArray<Job> tmp(2*job_.size());
  memmove(tmp.data(), job_.data(), njob_*sizeof job_[0]);
  job_ = std::move(tmp);
}

// Push (id, p) onto the stack, growing it if necessary.
void BitState::Push(int id, const char* p) {
  if (njob_ >= job_.size()) {
    GrowStack();
    if (njob_ >= job_.size()) {
      LOG(DFATAL) << "GrowStack() failed: "
                  << "njob_ = " << njob_ << ", "
                  << "job_.size() = " << job_.size();
      return;
    }
  }

  // If id < 0, it's undoing a Capture,
  // so we mustn't interfere with that.
  if (id >= 0 && njob_ > 0) {
    Job* top = &job_[njob_-1];
    if (id == top->id &&
        p == top->p + top->rle + 1 &&
        top->rle < std::numeric_limits<int>::max()) {
      ++top->rle;
      return;
    }
  }

  Job* top = &job_[njob_++];
  top->id = id;
  top->rle = 0;
  top->p = p;
}

// Bulk equivalent of Push(id, p), Push(id, p+1), ..., Push(id, p+n-1) with
// id >= 0 and n >= 1, which the run length encoding collapses into at most
// one new job (or a pure extension of the top job's run).
void BitState::PushRun(int id, const char* p, size_t n) {
  if (njob_ > 0) {
    Job* top = &job_[njob_-1];
    if (id == top->id &&
        p == top->p + top->rle + 1 &&
        n <= static_cast<size_t>(std::numeric_limits<int>::max() - top->rle)) {
      top->rle += static_cast<int>(n);
      return;
    }
  }

  if (njob_ >= job_.size()) {
    GrowStack();
    if (njob_ >= job_.size()) {
      LOG(DFATAL) << "GrowStack() failed: "
                  << "njob_ = " << njob_ << ", "
                  << "job_.size() = " << job_.size();
      return;
    }
  }

  Job* top = &job_[njob_++];
  top->id = id;
  top->rle = static_cast<int>(n - 1);
  top->p = p;
}

// Bulk equivalent of m consecutive ShouldVisit(id, ...) check-and-set calls
// for the positions pfirst, pfirst+1, ..., pfirst+m-1, all belonging to the
// instruction list `list`.  Returns m after setting all m bits if none were
// already set; otherwise returns the index k of the first already-set bit,
// having set only the k bits before it (mirroring where a sequential search
// would have stopped visiting).
size_t BitState::VisitRun(int list, const char* pfirst, size_t m) {
  size_t base = static_cast<size_t>(list) * (text_.size()+1) +
                static_cast<size_t>(pfirst - text_.data());
  size_t done = 0;
  while (done < m) {
    size_t bit = base + done;
    size_t word = bit / kVisitedBits;
    unsigned off = static_cast<unsigned>(bit % kVisitedBits);
    size_t span = m - done;
    if (span > kVisitedBits - off)
      span = kVisitedBits - off;
    uint64_t mask = span == kVisitedBits
                        ? ~uint64_t{0}
                        : ((uint64_t{1} << span) - 1) << off;
    uint64_t hit = visited_[word] & mask;
    if (hit != 0) {
      unsigned first = 0;
#if defined(__GNUC__) || defined(__clang__)
      first = static_cast<unsigned>(__builtin_ctzll(hit));
#else
      while (!(hit & (uint64_t{1} << first)))
        first++;
#endif
      // Set only the bits of this word that come before the first set bit.
      visited_[word] |= mask & ((uint64_t{1} << first) - 1);
      return done + (first - off);
    }
    visited_[word] |= mask;
    done += span;
  }
  return m;
}

// Like VisitRun, but only probes: returns the index of the first already-set
// bit among the m bits for pfirst, pfirst+1, ..., pfirst+m-1 in list `list`,
// or m if none is set.  Does not modify the bitmap.
size_t BitState::FirstVisited(int list, const char* pfirst, size_t m) {
  size_t base = static_cast<size_t>(list) * (text_.size()+1) +
                static_cast<size_t>(pfirst - text_.data());
  size_t done = 0;
  while (done < m) {
    size_t bit = base + done;
    size_t word = bit / kVisitedBits;
    unsigned off = static_cast<unsigned>(bit % kVisitedBits);
    size_t span = m - done;
    if (span > kVisitedBits - off)
      span = kVisitedBits - off;
    uint64_t mask = span == kVisitedBits
                        ? ~uint64_t{0}
                        : ((uint64_t{1} << span) - 1) << off;
    uint64_t hit = visited_[word] & mask;
    if (hit != 0) {
      unsigned first = 0;
#if defined(__GNUC__) || defined(__clang__)
      first = static_cast<unsigned>(__builtin_ctzll(hit));
#else
      while (!(hit & (uint64_t{1} << first)))
        first++;
#endif
      return done + (first - off);
    }
    done += span;
  }
  return m;
}

// Sets the m visited bits for pfirst, pfirst+1, ..., pfirst+m-1 in `list`.
void BitState::SetVisited(int list, const char* pfirst, size_t m) {
  size_t base = static_cast<size_t>(list) * (text_.size()+1) +
                static_cast<size_t>(pfirst - text_.data());
  size_t done = 0;
  while (done < m) {
    size_t bit = base + done;
    size_t word = bit / kVisitedBits;
    unsigned off = static_cast<unsigned>(bit % kVisitedBits);
    size_t span = m - done;
    if (span > kVisitedBits - off)
      span = kVisitedBits - off;
    uint64_t mask = span == kVisitedBits
                        ? ~uint64_t{0}
                        : ((uint64_t{1} << span) - 1) << off;
    visited_[word] |= mask;
    done += span;
  }
}

// Try a search from instruction id0 in state p0.
// Return whether it succeeded.
bool BitState::TrySearch(int id0, const char* p0) {
  bool matched = false;
  const char* end = text_.data() + text_.size();
  njob_ = 0;
  // Push() no longer checks ShouldVisit(),
  // so we must perform the check ourselves.
  if (ShouldVisit(id0, p0))
    Push(id0, p0);
  while (njob_ > 0) {
    // Pop job off stack.
    --njob_;
    int id = job_[njob_].id;
    int& rle = job_[njob_].rle;
    const char* p = job_[njob_].p;

    if (id < 0) {
      // Undo the Capture.
      cap_[prog_->inst(-id)->cap()] = p;
      continue;
    }

    if (rle > 0) {
      p += rle;
      // Revivify job on stack.
      --rle;
      ++njob_;
    }

  Loop:
    // Visit id, p.
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "Unexpected opcode: " << ip->opcode();
        return false;

      case kInstFail:
        break;

      case kInstAltMatch:
        if (ip->greedy(prog_)) {
          // out1 is the Match instruction.
          id = ip->out1();
          p = end;
          goto Loop;
        }
        if (longest_) {
          // ip must be non-greedy...
          // out is the Match instruction.
          id = ip->out();
          p = end;
          goto Loop;
        }
        goto Next;

      case kInstByteRange: {
        int c = -1;
        if (p < end)
          c = *p & 0xFF;
        if (!ip->Matches(c))
          goto Next;

        // Greedy self-loop acceleration: when this byte range jumps straight
        // back to the head of its own instruction list (the shape that x*,
        // x+ and character-class loops compile to), a maximal run of input
        // bytes that (a) match this range and (b) do not match any earlier
        // byte-range alternative in the list (which the sequential search
        // would have taken first) is consumed in bulk.  The per-position
        // backtrack alternatives collapse into a single RLE job - the same
        // encoding that per-position Push() calls would have produced - and
        // the visited bits for the run are set word-wise, stopping (exactly
        // like the per-position code) at the first already-visited position.
        {
          bool dead = false;
          do {
            if (ip->foldcase())
              break;
            // Two loop-back shapes: (A) this byte range's out points straight
            // at the head of its own list; (B) it points at a kInstNop (the
            // head of another list) whose out points back at the head of this
            // instruction's list, as x+ compiles to.  In shape B the nop's
            // list gets its own per-position visited checks and (if the nop
            // is not last in its list) per-position backtrack pushes.
            // Find the head of this instruction's list by walking back to
            // the previous list's last instruction (lists are short).
            // list_heads() is only meaningful for head ids.
            int h = id;
            while (h > 0 && !prog_->inst(h-1)->last())
              h--;
            int head = ip->out();
            int nopid = -1;
            if (head != h) {
              Prog::Inst* np = prog_->inst(head);
              if (np->opcode() != kInstNop || ip->hint() != 0)
                break;
              nopid = head;
              head = np->out();
              if (head != h)
                break;
            }
            // Collect the byte ranges of the alternatives that precede this
            // instruction in its list; bail out on anything non-trivial.
            int nb = 0;
            uint8_t blo[4], bhi[4];
            bool simple = true;
            for (int j = head; j < id; j++) {
              Prog::Inst* jp = prog_->inst(j);
              if (nb == 4 || jp->opcode() != kInstByteRange || jp->foldcase()) {
                simple = false;
                break;
              }
              blo[nb] = static_cast<uint8_t>(jp->lo());
              bhi[nb] = static_cast<uint8_t>(jp->hi());
              nb++;
            }
            if (!simple)
              break;
            const uint8_t lo = static_cast<uint8_t>(ip->lo());
            const uint8_t hi = static_cast<uint8_t>(ip->hi());
            const char* q = p + 1;
            while (q < end) {
              uint8_t cc = static_cast<uint8_t>(*q);
              if (cc < lo || cc > hi)
                break;
              bool earlier = false;
              for (int k = 0; k < nb; k++) {
                if (cc >= blo[k] && cc <= bhi[k]) {
                  earlier = true;
                  break;
                }
              }
              if (earlier)
                break;
              q++;
            }
            const size_t m = static_cast<size_t>(q - p);  // >= 1
            if (m == 1)
              break;  // no run to speak of; take the per-position path
            if (nopid < 0) {
              // Shape A.  Each consumed byte at pos pushes the backtrack
              // alternative at pos, then check-and-sets the visited bit for
              // pos+1, dying at the first already-set bit.
              const size_t k = VisitRun(prog_->list_heads()[h], p + 1, m);
              if (k < m) {
                // The sequential search would die at the already-visited
                // position, after pushing alternatives for the first k+1
                // bytes.
                if (ip->hint() != 0)
                  PushRun(id + ip->hint(), p, k + 1);
                dead = true;
                break;
              }
              if (ip->hint() != 0)
                PushRun(id + ip->hint(), p, m);
            } else {
              // Shape B.  Each consumed byte at pos check-and-sets the nop
              // list's visited bit for pos+1, pushes the alternative after
              // the nop (if any) at pos+1, then check-and-sets this list's
              // visited bit for pos+1; execution dies at the first
              // already-set bit, in that order.
              const int list_nop = prog_->list_heads()[nopid];
              const int list_head = prog_->list_heads()[h];
              const size_t kn = FirstVisited(list_nop, p + 1, m);
              const size_t kh = FirstVisited(list_head, p + 1, kn);
              const bool nop_push = !prog_->inst(nopid)->last();
              if (kh < kn) {
                // Dies at this list's check, after the nop list's
                // check-and-set and the nop push for the same position.
                SetVisited(list_nop, p + 1, kh + 1);
                SetVisited(list_head, p + 1, kh);
                if (nop_push)
                  PushRun(nopid + 1, p + 1, kh + 1);
                dead = true;
                break;
              }
              if (kn < m) {
                // Dies at the nop list's check.
                SetVisited(list_nop, p + 1, kn);
                SetVisited(list_head, p + 1, kn);
                if (nop_push && kn > 0)
                  PushRun(nopid + 1, p + 1, kn);
                dead = true;
                break;
              }
              SetVisited(list_nop, p + 1, m);
              SetVisited(list_head, p + 1, m);
              if (nop_push)
                PushRun(nopid + 1, p + 1, m);
            }
            id = head;
            p += m;
            // The run has already set the visited bit for (list, p),
            // just as the last per-position iteration would have.
            goto Loop;
          } while (0);
          if (dead)
            break;
        }

        if (ip->hint() != 0)
          Push(id+ip->hint(), p);  // try the next when we're done
        id = ip->out();
        p++;
        goto CheckAndLoop;
      }

      case kInstCapture:
        if (!ip->last())
          Push(id+1, p);  // try the next when we're done

        if (0 <= ip->cap() && ip->cap() < cap_.size()) {
          // Capture p to register, but save old value first.
          Push(-id, cap_[ip->cap()]);  // undo when we're done
          cap_[ip->cap()] = p;
        }

        id = ip->out();
        goto CheckAndLoop;

      case kInstEmptyWidth:
        if (ip->empty() & ~Prog::EmptyFlags(context_, p))
          goto Next;

        if (!ip->last())
          Push(id+1, p);  // try the next when we're done
        id = ip->out();
        goto CheckAndLoop;

      case kInstNop:
        if (!ip->last())
          Push(id+1, p);  // try the next when we're done
        id = ip->out();

      CheckAndLoop:
        // Sanity check: id is the head of its list, which must
        // be the case if id-1 is the last of *its* list. :)
        DCHECK(id == 0 || prog_->inst(id-1)->last());
        if (ShouldVisit(id, p))
          goto Loop;
        break;

      case kInstMatch: {
        if (endmatch_ && p != end)
          goto Next;

        // We found a match.  If the caller doesn't care
        // where the match is, no point going further.
        if (nsubmatch_ == 0)
          return true;

        // Record best match so far.
        // Only need to check end point, because this entire
        // call is only considering one start position.
        matched = true;
        cap_[1] = p;
        if (submatch_[0].data() == NULL ||
            (longest_ && p > submatch_[0].data() + submatch_[0].size())) {
          for (int i = 0; i < nsubmatch_; i++)
            submatch_[i] =
                StringPiece(cap_[2 * i],
                            static_cast<size_t>(cap_[2 * i + 1] - cap_[2 * i]));
        }

        // If going for first match, we're done.
        if (!longest_)
          return true;

        // If we used the entire text, no longer match is possible.
        if (p == end)
          return true;

        // Otherwise, continue on in hope of a longer match.
        // Note the absence of the ShouldVisit() check here
        // due to execution remaining in the same list.
      Next:
        if (!ip->last()) {
          id++;
          goto Loop;
        }
        break;
      }
    }
  }
  return matched;
}

// Search text (within context) for prog_.
bool BitState::Search(const StringPiece& text, const StringPiece& context,
                      bool anchored, bool longest,
                      StringPiece* submatch, int nsubmatch) {
  // Search parameters.
  text_ = text;
  context_ = context;
  if (context_.data() == NULL)
    context_ = text;
  if (prog_->anchor_start() && BeginPtr(context_) != BeginPtr(text))
    return false;
  if (prog_->anchor_end() && EndPtr(context_) != EndPtr(text))
    return false;
  anchored_ = anchored || prog_->anchor_start();
  longest_ = longest || prog_->anchor_end();
  endmatch_ = prog_->anchor_end();
  submatch_ = submatch;
  nsubmatch_ = nsubmatch;
  for (int i = 0; i < nsubmatch_; i++)
    submatch_[i] = StringPiece();

  // Allocate scratch space.
  int nvisited = prog_->list_count() * static_cast<int>(text.size()+1);
  nvisited = (nvisited + kVisitedBits-1) / kVisitedBits;
  visited_ = PODArray<uint64_t>(nvisited);
  memset(visited_.data(), 0, nvisited*sizeof visited_[0]);

  int ncap = 2*nsubmatch;
  if (ncap < 2)
    ncap = 2;
  cap_ = PODArray<const char*>(ncap);
  memset(cap_.data(), 0, ncap*sizeof cap_[0]);

  // When sizeof(Job) == 16, we start with a nice round 1KiB. :)
  job_ = PODArray<Job>(64);

  // Anchored search must start at text.begin().
  if (anchored_) {
    cap_[0] = text.data();
    return TrySearch(prog_->start(), text.data());
  }

  // Unanchored search, starting from each possible text position.
  // Notice that we have to try the empty string at the end of
  // the text, so the loop condition is p <= text.end(), not p < text.end().
  // This looks like it's quadratic in the size of the text,
  // but we are not clearing visited_ between calls to TrySearch,
  // so no work is duplicated and it ends up still being linear.
  const char* etext = text.data() + text.size();
  for (const char* p = text.data(); p <= etext; p++) {
    // Try to use prefix accel (e.g. memchr) to skip ahead.
    if (p < etext && prog_->can_prefix_accel()) {
      p = reinterpret_cast<const char*>(prog_->PrefixAccel(p, etext - p));
      if (p == NULL)
        p = etext;
    }

    cap_[0] = p;
    if (TrySearch(prog_->start(), p))  // Match must be leftmost; done.
      return true;
    // Avoid invoking undefined behavior (arithmetic on a null pointer)
    // by simply not continuing the loop.
    if (p == NULL)
      break;
  }
  return false;
}

// Bit-state search.
bool Prog::SearchBitState(const StringPiece& text,
                          const StringPiece& context,
                          Anchor anchor,
                          MatchKind kind,
                          StringPiece* match,
                          int nmatch) {
  // If full match, we ask for an anchored longest match
  // and then check that match[0] == text.
  // So make sure match[0] exists.
  StringPiece sp0;
  if (kind == kFullMatch) {
    anchor = kAnchored;
    if (nmatch < 1) {
      match = &sp0;
      nmatch = 1;
    }
  }

  // Run the search.
  BitState b(this);
  bool anchored = anchor == kAnchored;
  bool longest = kind != kFirstMatch;
  if (!b.Search(text, context, anchored, longest, match, nmatch))
    return false;
  if (kind == kFullMatch && EndPtr(match[0]) != EndPtr(text))
    return false;
  return true;
}

}  // namespace re2
