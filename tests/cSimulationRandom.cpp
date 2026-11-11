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

#include <cassert>

namespace stepraft {

namespace {

std::uint64_t RotateLeft(std::uint64_t value, int bits) {
  return (value << bits) | (value >> (64 - bits));
}

}  // namespace

SimulationRandom::SimulationRandom(std::uint64_t seed) {
  for (std::uint64_t &word : state_) {
    seed += 0x9E3779B97F4A7C15ull;
    std::uint64_t z = seed;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    word = z ^ (z >> 31);
  }
}

std::uint64_t SimulationRandom::Next() {
  const std::uint64_t result = RotateLeft(state_[1] * 5, 7) * 9;
  const std::uint64_t shifted = state_[1] << 17;
  state_[2] ^= state_[0];
  state_[3] ^= state_[1];
  state_[1] ^= state_[2];
  state_[0] ^= state_[3];
  state_[2] ^= shifted;
  state_[3] = RotateLeft(state_[3], 45);
  return result;
}

std::uint64_t SimulationRandom::Below(std::uint64_t bound) {
  assert(bound > 0);
  // Rejection keeps the result unbiased for any bound.
  const std::uint64_t threshold = (0 - bound) % bound;
  for (;;) {
    const std::uint64_t value = Next();
    if (value >= threshold) return value % bound;
  }
}

std::uint64_t SimulationRandom::Between(std::uint64_t low, std::uint64_t high) {
  assert(low <= high);
  return low + Below(high - low + 1);
}

bool SimulationRandom::Chance(std::uint32_t partsPerMillion) {
  if (partsPerMillion == 0) return false;
  if (partsPerMillion >= 1'000'000) return true;
  return Below(1'000'000) < partsPerMillion;
}

}  // namespace stepraft
