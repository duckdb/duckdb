#include "storage/single_file_block_manager.hpp"

#include "common/exception.hpp"

#include "common/file_system.hpp"

#include <unistd.h>
#include <fcntl.h>

using namespace duckdb;
using namespace std;


SingleFileBlockManager::SingleFileBlockManager(string path, bool read_only) :
	path(path){
	int rc;

	if (FileSystem::FileExists(path)) {
		if (read_only) {
			// open the file for reading
			fd = open(path.c_str(), O_RDONLY);
			if (fd == -1) {
				throw IOException("Cannot open database file \"%s\": %s", path.c_str(), strerror(errno));
			}
			// set a read lock on the file
			struct flock fl;
			memset(&fl, 0, sizeof fl);
			fl.l_type = F_RDLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = 0;
			rc = fcntl(fd, F_SETLK, &fl);
			if (rc == -1) {
				throw IOException("Could not lock file \"%s\" for reading: %s", path.c_str(), strerror(errno));
			}
		} else {
			// open the file for writing
			// we use direct IO here
#if defined(__DARWIN__) || defined(__APPLE__)
			// OSX: no O_DIRECT
			fd = open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_SYNC);
			if (fd == -1) {
				throw IOException("Cannot open database file \"%s\": %s", path.c_str(), strerror(errno));
			}
			// get direct IO with fcntl instead
			rc = fcntl(fd, F_NOCACHE, 1);
			if (fd == -1) {
				throw IOException("Could not enable direct IO for database file \"%s\": %s", path.c_str(), strerror(errno));
			}
#else
			// Linux: use O_DIRECT
			fd = open(path.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_CLOEXEC | O_SYNC);
			if (fd == -1) {
				throw IOException("Cannot open database file \"%s\": %s", path.c_str(), strerror(errno));
			}
#endif
			// set a write lock on the file
			struct flock fl;
			memset(&fl, 0, sizeof fl);
			fl.l_type = F_WRLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = 0;
			rc = fcntl(fd, F_SETLK, &fl);
			if (rc == -1) {
				throw IOException("Could not lock file \"%s\" for writing: %s", path.c_str(), strerror(errno));
			}
		}
	} else {

	}
}

unique_ptr<Block> SingleFileBlockManager::GetBlock(block_id_t id) {
	return nullptr;
}

string SingleFileBlockManager::GetBlockPath(block_id_t id) {
	return "";
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock() {
	return nullptr;
}

void SingleFileBlockManager::Flush(unique_ptr<Block> &block) {

}

void SingleFileBlockManager::WriteHeader(int64_t version, block_id_t meta_block) {

}
