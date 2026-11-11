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

#include "cSimulationRandom.hpp"

#include <gtest/gtest.h>

namespace stepraft {
namespace {

// Reference values from an independent implementation of the same
// generator: a mismatch on any platform breaks replay by seed.
TEST(SimulationRandomTest, MatchesReferenceSequence) {
  SimulationRandom random(42);
  EXPECT_EQ(random.Next(), 0x15780b2e0c2ec716ull);
  EXPECT_EQ(random.Next(), 0x6104d9866d113a7eull);
  EXPECT_EQ(random.Next(), 0xae17533239e499a1ull);

  SimulationRandom bounded(7);
  const std::uint64_t expected[] = {4, 4, 8, 4, 4};
  for (const std::uint64_t value : expected)
    EXPECT_EQ(bounded.Below(10), value);
}

TEST(SimulationRandomTest, SameSeedSameStreamOtherSeedOtherStream) {
  SimulationRandom a(1), b(1), c(2);
  bool differs = false;
  for (int i = 0; i < 100; ++i) {
    const std::uint64_t value = a.Next();
    EXPECT_EQ(value, b.Next());
    differs = differs || value != c.Next();
  }
  EXPECT_TRUE(differs);
}

TEST(SimulationRandomTest, RangesAndChances) {
  SimulationRandom random(3);
  for (int i = 0; i < 1000; ++i) {
    EXPECT_LT(random.Below(7), 7u);
    const std::uint64_t value = random.Between(5, 9);
    EXPECT_GE(value, 5u);
    EXPECT_LE(value, 9u);
  }
  EXPECT_EQ(random.Below(1), 0u);
  EXPECT_EQ(random.Between(4, 4), 4u);
  EXPECT_FALSE(random.Chance(0));
  EXPECT_TRUE(random.Chance(1'000'000));
}

}  // namespace
}  // namespace stepraft
