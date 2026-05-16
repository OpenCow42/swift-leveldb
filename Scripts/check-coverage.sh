#!/usr/bin/env bash
set -euo pipefail

swift test --enable-code-coverage

binary="$(find .build -path '*/swift-leveldbPackageTests.xctest/Contents/MacOS/swift-leveldbPackageTests' -type f | head -n 1)"
profile="$(find .build -path '*/codecov/default.profdata' -type f | head -n 1)"

if [[ -z "${binary}" || -z "${profile}" ]]; then
  echo "Unable to locate SwiftPM coverage artifacts." >&2
  exit 1
fi

report="$(xcrun llvm-cov report "${binary}" \
  -instr-profile="${profile}" \
  -ignore-filename-regex='/.build/|/Vendor/|/Tests/|/Benchmarks/|/Sources/SwiftLevelDBBenchCore/')"

echo "${report}"

line_coverage="$(printf '%s\n' "${report}" | awk '/^TOTAL/ { print $10 }')"
if [[ "${line_coverage}" != "100.00%" ]]; then
  echo "Expected 100.00% Swift library line coverage, got ${line_coverage}." >&2
  exit 1
fi
