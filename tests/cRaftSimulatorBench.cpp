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
#include <vector>

#include "cRaftSimulator.hpp"

namespace stepraft {
namespace {

struct BenchArguments {
  std::uint64_t seeds = 1000;
  std::uint64_t ticks = 1000;
  std::uint32_t nodes = 3;
};

bool Parse(int argc, char **argv, BenchArguments &arguments) {
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::uint64_t value = std::strtoull(argv[i + 1], nullptr, 10);
    if (std::strcmp(argv[i], "--seeds") == 0) {
      arguments.seeds = value;
    } else if (std::strcmp(argv[i], "--ticks") == 0) {
      arguments.ticks = value;
    } else if (std::strcmp(argv[i], "--nodes") == 0) {
      arguments.nodes = static_cast<std::uint32_t>(value);
    } else {
      return false;
    }
  }
  return argc % 2 == 1 && arguments.nodes >= 3 && arguments.seeds > 0;
}

SimulationOptions Chaos(std::uint64_t seed, std::uint32_t nodes) {
  SimulationOptions options;
  options.seed = seed;
  options.members.clear();
  for (std::uint32_t id = 1; id <= nodes; ++id) options.members.push_back(id);
  options.dropPpm = 50'000;
  options.duplicatePpm = 20'000;
  options.delayMaxTicks = 5;
  options.diskDelayMaxTicks = 4;
  options.tickJitterTicks = 1;
  options.crashPpm = 5'000;
  options.maxNodesDown = (nodes - 1) / 2;
  options.restartDelayMinTicks = 5;
  options.restartDelayMaxTicks = 30;
  options.trace = false;
  return options;
}

}  // namespace
}  // namespace stepraft

int main(int argc, char **argv) {
  using namespace stepraft;
  BenchArguments arguments;
  if (!Parse(argc, argv, arguments)) {
    std::fprintf(stderr, "usage: %s [--seeds N] [--ticks N] [--nodes N>=3]\n",
                 argv[0]);
    return 2;
  }
  std::uint64_t events = 0;
  std::uint64_t committed = 0;
  std::uint64_t mix = 0;
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t seed = 1; seed <= arguments.seeds; ++seed) {
    RaftSimulator sim(Chaos(seed, arguments.nodes));
    // One small entry offered every tick; only a leader takes it.
    for (std::uint64_t tick = 0; tick < arguments.ticks; ++tick) {
      for (const std::uint32_t id : sim.Members())
        if (sim.Propose(id, std::vector<std::uint8_t>(64, 0x5A))) break;
      if (!sim.RunFor(1)) {
        std::fprintf(stderr, "%s", sim.Report().c_str());
        return 1;
      }
    }
    committed += sim.Core(sim.Members().front())
                     ? sim.Core(sim.Members().front())->CommitIndex()
                     : 0;
    events += sim.Counters().events;
    mix ^= sim.Digest();
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();
  std::printf(
      "runs=%llu ticks/run=%llu nodes=%u events=%llu committed=%llu "
      "total=%.3fs per-run=%.1fus events/s=%.0f digest-mix=%016llx\n",
      static_cast<unsigned long long>(arguments.seeds),
      static_cast<unsigned long long>(arguments.ticks), arguments.nodes,
      static_cast<unsigned long long>(events),
      static_cast<unsigned long long>(committed), seconds,
      seconds * 1e6 / static_cast<double>(arguments.seeds),
      static_cast<double>(events) / seconds,
      static_cast<unsigned long long>(mix));
  return 0;
}
