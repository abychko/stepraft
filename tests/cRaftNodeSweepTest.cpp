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

#include <gtest/gtest.h>

#include <cstdio>

#include "cRaftSweep.hpp"

namespace stepraft {
namespace {

TEST(RaftNodeSweepTest, SeededRunsKeepRaftSafetyAndSettle) {
  for (const std::uint32_t nodes : {3u, 5u}) {
    SweepCoverage coverage;
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
      const auto broken = RaftSweep::RunSeed(nodes, seed, 3000, coverage);
      ASSERT_FALSE(broken) << nodes << " nodes, seed " << seed << "\n"
                           << *broken;
    }
    EXPECT_EQ(coverage.runs, 100u);
    EXPECT_GT(coverage.leaderships, 2 * coverage.runs) << nodes << " nodes";
    EXPECT_GT(coverage.runsWithChange, coverage.runs * 9 / 10)
        << nodes << " nodes";
    EXPECT_GT(coverage.crashes, coverage.runs) << nodes << " nodes";
    EXPECT_GT(coverage.truncations, coverage.runs / 10) << nodes << " nodes";
    EXPECT_GT(coverage.transfers, coverage.runs) << nodes << " nodes";
    EXPECT_GT(coverage.committed, 100 * coverage.runs) << nodes << " nodes";
  }
}

TEST(RaftNodeSweepTest, SeededRunsWithReplacedMembersKeepRaftSafety) {
  for (const std::uint32_t nodes : {3u, 5u}) {
    SweepCoverage coverage;
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
      const auto broken =
          RaftSweep::RunSeed(nodes, seed, 3000, coverage, {.membership = true});
      ASSERT_FALSE(broken) << nodes << " nodes, seed " << seed << "\n"
                           << *broken;
    }
    EXPECT_EQ(coverage.runs, 100u);
    EXPECT_GT(coverage.wipes, coverage.runs) << nodes << " nodes";
    EXPECT_EQ(coverage.replacements, coverage.wipes) << nodes << " nodes";
    EXPECT_GT(coverage.committed, 100 * coverage.runs) << nodes << " nodes";
  }
}

TEST(RaftNodeSweepTest, SeededRunsWithEvictionKeepRaftSafety) {
  for (const bool membership : {false, true}) {
    for (const std::uint32_t nodes : {3u, 5u}) {
      SweepCoverage coverage;
      for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        const auto broken =
            RaftSweep::RunSeed(nodes, seed, 3000, coverage,
                               {.membership = membership, .eviction = true});
        ASSERT_FALSE(broken)
            << nodes << " nodes, seed " << seed
            << (membership ? ", with replacements" : "") << "\n"
            << *broken;
      }
      EXPECT_EQ(coverage.runs, 100u);
      EXPECT_GT(coverage.evictions, 10 * coverage.runs) << nodes << " nodes";
      EXPECT_GT(coverage.resets, coverage.runs) << nodes << " nodes";
      EXPECT_GT(coverage.committed, 100 * coverage.runs) << nodes << " nodes";
    }
  }
}

// Seeds found by large sweeps that broke a weakened core, kept here so
// that ctest catches the same weakening without a large sweep. 11091:
// a Ready is acknowledged after a later one already replaced its tail
// (an executor that comes back late); the core must not treat the
// replaced tail as written.
TEST(RaftNodeSweepTest, SeedsThatOnceCaughtAWeakenedCoreStaySafe) {
  SweepCoverage coverage;
  for (const std::uint64_t seed : {11091u}) {
    const auto broken = RaftSweep::RunSeed(3, seed, 3000, coverage);
    ASSERT_FALSE(broken) << "seed " << seed << "\n" << *broken;
  }
}

}  // namespace
}  // namespace stepraft
