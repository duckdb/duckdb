#include "re2/prog.h"

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

	void Reset() {
		anchored_ = false;
		longest_ = false;
		endmatch_ =false;
		submatch_ =NULL;
		nsubmatch_ =0;
		njob_= 0;
	}

private:
	inline bool ShouldVisit(int id, const char* p);
	void Push(int id, const char* p);
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

}
