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
#include <cstdint>

namespace stepraft {

struct SimulationCounters {
  std::uint64_t events = 0;
  std::uint64_t ticks = 0;
  std::uint64_t sent = 0;
  std::uint64_t delivered = 0;
  // By the fault model, a partition, or a receiver that is down.
  std::uint64_t dropped = 0;
  std::uint64_t duplicated = 0;
  std::uint64_t diskWrites = 0;
  // Readies that cut existing entries off a log.
  std::uint64_t truncations = 0;
  // Followers that restarted their log at the leader's start.
  std::uint64_t resets = 0;
  std::uint64_t crashes = 0;
  std::uint64_t restarts = 0;
};

}  // namespace stepraft
