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

#pragma once
#include <array>
#include <cstdint>

namespace stepraft {

// xoshiro256** seeded through splitmix64, written out instead of the
// <random> distributions, whose results differ between standard
// libraries: one seed must replay one run on every platform.
class SimulationRandom {
 public:
  explicit SimulationRandom(std::uint64_t seed);

  std::uint64_t Next();
  // Uniform in [0, bound); bound must be positive.
  std::uint64_t Below(std::uint64_t bound);
  // Uniform in [low, high].
  std::uint64_t Between(std::uint64_t low, std::uint64_t high);
  // True with probability partsPerMillion / 1'000'000.
  bool Chance(std::uint32_t partsPerMillion);

 private:
  std::array<std::uint64_t, 4> state_{};
};

}  // namespace stepraft
