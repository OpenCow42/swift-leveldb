// Benchmark-only shim for vendored LevelDB's db_bench.cc.
//
// Upstream db_bench.cc uses only leveldb::test::CompressibleString from
// util/testutil.h. The real upstream header also depends on gtest/gmock, which
// are not vendored in this SwiftPM package.

#ifndef SWIFT_LEVELDB_BENCHMARK_TESTUTIL_SHIM_H_
#define SWIFT_LEVELDB_BENCHMARK_TESTUTIL_SHIM_H_

#include <string>

#include "leveldb/slice.h"
#include "util/random.h"

namespace leveldb {
namespace test {

Slice CompressibleString(Random* rnd, double compressed_fraction, size_t len,
                         std::string* dst);

}  // namespace test
}  // namespace leveldb

#endif  // SWIFT_LEVELDB_BENCHMARK_TESTUTIL_SHIM_H_
