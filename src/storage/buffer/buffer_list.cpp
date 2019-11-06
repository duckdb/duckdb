#include "duckdb/storage/buffer/buffer_list.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BufferEntry> BufferList::Pop() {
	if (!root) {
		// no root: return nullptr
		return nullptr;
	}
	// fetch root
	auto entry = move(root);
	root = move(entry->next);
	if (root) {
		// new root no longer has prev pointer
		root->prev = nullptr;
	} else {
		last = nullptr;
	}
	count--;
	return entry;
}

unique_ptr<BufferEntry> BufferList::Erase(BufferEntry *entry) {
	assert(entry->prev || entry == root.get());
	assert(entry->next || entry == last);
	// first get the entry, either from the previous entry or from the root node
	auto current = entry->prev ? move(entry->prev->next) : move(root);
	auto prev = entry->prev;
	if (entry == last) {
		// entry was last entry: last is now the previous entry
		last = prev;
	}
	// now set up prev/next pointers correctly
	auto next = move(entry->next);
	if (!prev) {
		// no prev: entry was root
		root = move(next);
		if (root) {
			// new root no longer has prev pointer
			root->prev = nullptr;
		} else {
			last = nullptr;
		}
		assert(!root || !root->prev);
	} else if (prev != last) {
		assert(next);
		next->prev = prev;
		prev->next = move(next);
	}
	count--;
	return current;
}

void BufferList::Append(unique_ptr<BufferEntry> entry) {
	assert(!entry->next);
	if (!last) {
		// empty list: set as root
		entry->prev = nullptr;
		root = move(entry);
		last = root.get();
	} else {
		// non-empty list: append to last entry and set entry as last
		entry->prev = last;
		last->next = move(entry);
		last = last->next.get();
	}
	count++;
}
