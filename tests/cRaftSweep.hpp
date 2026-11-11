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
#include <optional>
#include <string>
#include "sSweepCoverage.hpp"

namespace stepraft {

class RaftTraceWriter;

// What a sweep run does besides proposals and network changes.
struct SweepPlan {
  // Now and then a node is wiped and the leader replaces it by its
  // membership steps: the old voter removed, then the new one added.
  bool membership = false;
  // Every node now and then evicts what it durably committed but its last
  // few entries, so a node away comes back behind the log.
  bool eviction = false;
  bool transfers = true;
  bool crashes = true;
  RaftTraceWriter *trace = nullptr;
};

class RaftSweep {
 public:
  // One seeded run: proposals all along, network changes, crashes, now
  // and then a leadership transfer; the simulator's checks run after
  // every event and log matching every 100 ticks. Then the faults stop
  // and the cluster has to elect a leader, commit a last entry everywhere
  // and agree on one log. Returns what went wrong, with the run's report.
  static std::optional<std::string> RunSeed(std::uint32_t nodes,
                                            std::uint64_t seed,
                                            std::uint64_t ticks,
                                            SweepCoverage &coverage,
                                            const SweepPlan &plan = {});
};

}  // namespace stepraft
