/* Copyright (c) 2026, Alexey Bychko.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of stepraft hereby grant you an
   additional permission to link the program and your derivative works
   with the separately licensed software that they have either included
   with the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "cRaftSweep.hpp"

namespace stepraft {
namespace {

struct SweepArguments {
  std::uint32_t nodes = 3;
  std::uint64_t firstSeed = 1;
  std::uint64_t seeds = 1000;
  std::uint64_t ticks = 3000;
};

bool Parse(int argc, char **argv, SweepArguments &arguments) {
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::uint64_t value = std::strtoull(argv[i + 1], nullptr, 10);
    if (std::strcmp(argv[i], "--nodes") == 0) {
      arguments.nodes = static_cast<std::uint32_t>(value);
    } else if (std::strcmp(argv[i], "--first-seed") == 0) {
      arguments.firstSeed = value;
    } else if (std::strcmp(argv[i], "--seeds") == 0) {
      arguments.seeds = value;
    } else if (std::strcmp(argv[i], "--ticks") == 0) {
      arguments.ticks = value;
    } else {
      return false;
    }
  }
  return argc % 2 == 1 && arguments.nodes >= 3 && arguments.seeds > 0;
}

}  // namespace
}  // namespace stepraft

int main(int argc, char **argv) {
  using namespace stepraft;
  SweepArguments arguments;
  if (!Parse(argc, argv, arguments)) {
    std::fprintf(stderr,
                 "usage: %s [--nodes N>=3] [--first-seed N] [--seeds N] "
                 "[--ticks N]\n",
                 argv[0]);
    return 2;
  }
  SweepCoverage coverage;
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t seed = arguments.firstSeed;
       seed < arguments.firstSeed + arguments.seeds; ++seed) {
    if (const auto broken = RaftSweep::RunSeed(arguments.nodes, seed,
                                               arguments.ticks, coverage)) {
      std::fprintf(stderr, "nodes=%u seed=%llu\n%s", arguments.nodes,
                   static_cast<unsigned long long>(seed), broken->c_str());
      return 1;
    }
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();
  std::printf(
      "nodes=%u seeds=%llu..%llu ticks=%llu runs=%llu leaderships=%llu "
      "runs-with-change=%llu crashes=%llu truncations=%llu transfers=%llu "
      "committed=%llu total=%.1fs\n",
      arguments.nodes, static_cast<unsigned long long>(arguments.firstSeed),
      static_cast<unsigned long long>(arguments.firstSeed + arguments.seeds -
                                      1),
      static_cast<unsigned long long>(arguments.ticks),
      static_cast<unsigned long long>(coverage.runs),
      static_cast<unsigned long long>(coverage.leaderships),
      static_cast<unsigned long long>(coverage.runsWithChange),
      static_cast<unsigned long long>(coverage.crashes),
      static_cast<unsigned long long>(coverage.truncations),
      static_cast<unsigned long long>(coverage.transfers),
      static_cast<unsigned long long>(coverage.committed), seconds);
  return 0;
}
