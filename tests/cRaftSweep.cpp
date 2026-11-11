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

#include "cRaftSweep.hpp"

#include <algorithm>

#include <set>
#include <utility>

#include "hRaftScenario.hpp"

namespace stepraft {

using namespace scenario;

namespace {

std::optional<std::uint32_t> CurrentLeader(const RaftSimulator &sim) {
  for (const std::uint32_t id : sim.Members())
    if (const RaftNode *core = sim.Core(id);
        core && core->Role() == RaftRole::Leader)
      return id;
  return std::nullopt;
}

// A wiped node and the voter it is to become.
struct Replacement {
  std::uint32_t id = 0;
  std::uint64_t old = 0;
  std::uint64_t fresh = 0;
};

// The leader takes its next membership step, if it has one; true once
// the new voter is in its membership.
bool Replace(RaftSimulator &sim, std::uint32_t leader,
             const Replacement &replacement) {
  const RaftNode &core = *sim.Core(leader);
  if (const auto step = core.MembershipStep())
    sim.ChangeMembership(leader, *step);
  const std::vector<RaftVoter> &voters = sim.Core(leader)->Voters();
  return std::find(voters.begin(), voters.end(),
                   RaftVoter{replacement.id, replacement.fresh}) !=
         voters.end();
}

}  // namespace

std::optional<std::string> RaftSweep::RunSeed(std::uint32_t nodes,
                                              std::uint64_t seed,
                                              std::uint64_t ticks,
                                              SweepCoverage &coverage,
                                              const SweepPlan &plan) {
  const bool membership = plan.membership;
  const bool eviction = plan.eviction;
  SimulationOptions options = Chaos(seed, nodes);
  if (!plan.crashes) options.crashPpm = 0;
  options.traceWriter = plan.trace;
  RaftSimulator sim(options);
  std::optional<Replacement> replacing;
  std::set<std::pair<std::uint64_t, std::uint32_t>> leaderships;
  sim.AddInvariant("coverage", [&leaderships](const RaftSimulator &s) {
    for (const std::uint32_t id : s.Members())
      if (const RaftNode *core = s.Core(id);
          core && core->Role() == RaftRole::Leader)
        leaderships.emplace(core->Term(), id);
    return std::optional<std::string>();
  });
  SimulationRandom script(seed ^ 0x5eed5eed5eedull);
  std::uint64_t proposal = 0;
  for (std::uint64_t elapsed = 0; elapsed < ticks; elapsed += 10) {
    if (elapsed % 100 == 0) {
      ShakeNetwork(sim, script);
      if (const auto broken = LogMatchingViolation(sim)) {
        return *broken + "\n" + sim.Report();
      }
    }
    if (plan.transfers && elapsed % 500 == 250) {
      for (const std::uint32_t id : sim.Members()) {
        if (const RaftNode *core = sim.Core(id);
            core && core->Role() == RaftRole::Leader) {
          sim.TransferLeadership(
              id, sim.Members()[script.Below(sim.Members().size())]);
          ++coverage.transfers;
        }
      }
    }
    if (membership && elapsed % 700 == 350 && !replacing) {
      // Wiped only once every member votes: a second loss in a cluster of
      // three under way is a lost quorum, declared.
      if (const auto leader = CurrentLeader(sim);
          leader &&
          sim.Core(*leader)->Voters().size() == sim.Members().size()) {
        const std::uint32_t id =
            sim.Members()[script.Below(sim.Members().size())];
        if (id != *leader && sim.IsUp(id)) {
          const std::uint64_t old = sim.StorageOf(id);
          sim.Wipe(id);
          sim.Restart(id);
          replacing = Replacement{id, old, sim.StorageOf(id)};
          ++coverage.wipes;
        }
      }
    }
    if (eviction && elapsed % 300 == 200) {
      for (const std::uint32_t id : sim.Members()) {
        const RaftNode *core = sim.Core(id);
        if (!core) continue;
        const std::uint64_t keep = 2 + script.Below(6);
        const std::uint64_t commit = sim.Log(id).HardState().commitIndex;
        if (commit > keep && commit - keep >= core->FirstIndex() &&
            sim.Evict(id, commit - keep))
          ++coverage.evictions;
      }
    }
    if (membership) {
      if (const auto leader = CurrentLeader(sim);
          leader && Replace(sim, *leader, replacing.value_or(Replacement{})) &&
          replacing) {
        replacing.reset();
        ++coverage.replacements;
      }
    }
    for (const std::uint32_t id : sim.Members())
      if (sim.Propose(id, Payload(++proposal))) break;
    if (!sim.RunFor(10)) {
      return sim.Report();
    }
  }

  sim.StopFaults();
  for (const std::uint32_t id : sim.Members())
    if (!sim.IsUp(id)) sim.Restart(id);
  std::optional<std::uint32_t> leader = RunUntilLeader(sim, 300);
  // The membership steps under way end with the cluster settled.
  const auto pending = [&] {
    return replacing || (membership && leader &&
                         sim.Core(*leader)->MembershipStep().has_value());
  };
  for (int step = 0; pending() && leader && step < 100; ++step) {
    if (Replace(sim, *leader, replacing.value_or(Replacement{})) && replacing) {
      replacing.reset();
      ++coverage.replacements;
    } else if (!sim.RunFor(10)) {
      return sim.Report();
    }
    leader = RunUntilLeader(sim, 300);
  }
  if (pending())
    return "the membership steps did not finish\n" + DescribeNodes(sim) +
           sim.Report();
  if (!leader || !sim.Propose(*leader, Payload(++proposal)) ||
      !sim.RunFor(100)) {
    return "the ring did not settle\n" + DescribeNodes(sim) + sim.Report();
  }
  if (const auto difference = LogDifference(sim)) {
    return "logs differ after settling: " + *difference + "\n" +
           DescribeNodes(sim) + sim.Report();
  }
  for (const std::uint32_t id : sim.Members()) {
    if (sim.Core(id)->CommitIndex() != sim.Core(*leader)->LastIndex()) {
      return "node " + std::to_string(id) + " did not commit the last entry\n" +
             sim.Report();
    }
  }
  ++coverage.runs;
  coverage.leaderships += leaderships.size();
  if (leaderships.size() > 1) ++coverage.runsWithChange;
  coverage.crashes += sim.Counters().crashes;
  coverage.truncations += sim.Counters().truncations;
  coverage.resets += sim.Counters().resets;
  coverage.committed += sim.Core(*leader)->CommitIndex();
  return std::nullopt;
}

}  // namespace stepraft
