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

#include "cRaftSimulator.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <sstream>

#include "cRaftTraceWriter.hpp"

namespace stepraft {
namespace {

// Injected vote requests: a higher term makes the receiver persist it.
RaftMessage Message(std::uint32_t from, std::uint32_t to, std::uint64_t term) {
  RaftMessage message;
  message.type = RaftMessageType::VoteRequest;
  message.from = from;
  message.to = to;
  message.term = term;
  return message;
}

// Elections never start, so the only traffic is what a test injects and
// the replies to it.
SimulationOptions Calm(std::uint64_t seed = 1) {
  SimulationOptions options;
  options.seed = seed;
  options.raft.electionTimeoutMinTicks = 1'000'000;
  options.raft.electionTimeoutMaxTicks = 1'000'000;
  return options;
}

SimulationOptions Chaos(std::uint64_t seed) {
  SimulationOptions options;
  options.seed = seed;
  options.dropPpm = 100'000;
  options.duplicatePpm = 100'000;
  options.delayMinTicks = 1;
  options.delayMaxTicks = 10;
  options.diskDelayMinTicks = 1;
  options.diskDelayMaxTicks = 8;
  options.tickJitterTicks = 2;
  options.crashPpm = 20'000;
  options.restartDelayMinTicks = 3;
  options.restartDelayMaxTicks = 15;
  return options;
}

// Terms of the injected requests that reached `node` with `kind`.
std::vector<std::uint64_t> RequestTerms(const RaftSimulator &sim,
                                        SimulationEventKind kind,
                                        std::uint32_t node) {
  std::vector<std::uint64_t> terms;
  for (const SimulationTraceEvent &event : sim.Trace())
    if (event.kind == kind && event.node == node &&
        event.type == RaftMessageType::VoteRequest)
      terms.push_back(event.term);
  return terms;
}

void DriveChaos(RaftSimulator &sim) {
  for (std::uint64_t step = 0; step < 200; ++step) {
    const auto from = static_cast<std::uint32_t>(step % 3) + 1;
    sim.Send(Message(from, from % 3 + 1, step / 4 + 1));
    ASSERT_TRUE(sim.RunFor(5)) << sim.Report();
  }
}

SimulationOptions Untraced(SimulationOptions options) {
  options.trace = false;
  return options;
}

TEST(RaftSimulatorTest, EveryNodeTicksOncePerTickWithoutJitter) {
  RaftSimulator sim(Calm());
  ASSERT_TRUE(sim.RunFor(10));
  EXPECT_EQ(sim.Now(), 10u);
  EXPECT_EQ(sim.Counters().ticks, 30u);
}

TEST(RaftSimulatorTest, CalmNetworkDeliversEverything) {
  RaftSimulator sim(Calm());
  sim.Send(Message(1, 2, 5));
  ASSERT_TRUE(sim.RunFor(1));
  EXPECT_EQ(sim.Counters().delivered, 1u);
  EXPECT_EQ(sim.Core(2)->Term(), 5u);
  EXPECT_EQ(sim.Core(3)->Term(), 0u);
}

TEST(RaftSimulatorTest, DropsAndDuplicatesFollowTheOptions) {
  SimulationOptions lossy = Calm();
  lossy.dropPpm = 1'000'000;
  RaftSimulator dropping(lossy);
  for (std::uint64_t term = 1; term <= 50; ++term)
    dropping.Send(Message(1, 2, term));
  ASSERT_TRUE(dropping.RunFor(5));
  EXPECT_EQ(RequestTerms(dropping, SimulationEventKind::Drop, 2).size(), 50u);
  EXPECT_TRUE(RequestTerms(dropping, SimulationEventKind::Deliver, 2).empty());
  EXPECT_EQ(dropping.Core(2)->Term(), 0u);

  SimulationOptions echoing = Calm();
  echoing.duplicatePpm = 1'000'000;
  RaftSimulator duplicating(echoing);
  for (std::uint64_t term = 1; term <= 20; ++term)
    duplicating.Send(Message(1, 2, term));
  ASSERT_TRUE(duplicating.RunFor(5));
  EXPECT_EQ(RequestTerms(duplicating, SimulationEventKind::Duplicate, 2).size(),
            20u);
  EXPECT_EQ(RequestTerms(duplicating, SimulationEventKind::Deliver, 2).size(),
            40u);
}

TEST(RaftSimulatorTest, DelaySpreadReordersWithoutLoss) {
  SimulationOptions options = Calm();
  options.delayMaxTicks = 20;
  RaftSimulator sim(options);
  for (std::uint64_t term = 1; term <= 50; ++term)
    sim.Send(Message(1, 2, term));
  ASSERT_TRUE(sim.RunFor(30));
  const auto terms = RequestTerms(sim, SimulationEventKind::Deliver, 2);
  EXPECT_EQ(terms.size(), 50u);
  EXPECT_FALSE(std::is_sorted(terms.begin(), terms.end()));
  EXPECT_EQ(sim.Core(2)->Term(), 50u);
}

TEST(RaftSimulatorTest, PartitionCutsSendsAndMessagesInFlight) {
  RaftSimulator sim(Calm());
  sim.Partition({{1}, {2, 3}});
  sim.Send(Message(1, 2, 3));
  sim.Send(Message(3, 2, 4));
  ASSERT_TRUE(sim.RunFor(2));
  EXPECT_EQ(sim.Core(2)->Term(), 4u);

  sim.Heal();
  sim.Send(Message(1, 2, 9));
  ASSERT_TRUE(sim.RunFor(2));
  EXPECT_EQ(sim.Core(2)->Term(), 9u);

  sim.Send(Message(1, 2, 11));
  sim.Partition({{1}, {2, 3}});
  ASSERT_TRUE(sim.RunFor(2));
  EXPECT_EQ(sim.Core(2)->Term(), 9u);
  EXPECT_EQ(RequestTerms(sim, SimulationEventKind::Drop, 2),
            (std::vector<std::uint64_t>{3, 11}));
}

TEST(RaftSimulatorTest, CutIsOneLinkBothWays) {
  RaftSimulator sim(Calm());
  sim.Cut(2, 1);
  sim.Send(Message(1, 2, 3));
  sim.Send(Message(3, 2, 4));
  sim.Send(Message(2, 1, 5));
  ASSERT_TRUE(sim.RunFor(1));
  EXPECT_EQ(sim.Core(2)->Term(), 4u);
  EXPECT_EQ(sim.Core(1)->Term(), 0u);
  sim.Mend(1, 2);
  sim.Send(Message(1, 2, 6));
  ASSERT_TRUE(sim.RunFor(1));
  EXPECT_EQ(sim.Core(2)->Term(), 6u);
}

TEST(RaftSimulatorTest, CrashLosesWhatTheDiskHasNotConfirmed) {
  SimulationOptions options = Calm();
  options.diskDelayMinTicks = 5;
  options.diskDelayMaxTicks = 5;

  RaftSimulator early(options);
  early.Send(Message(1, 2, 7));
  ASSERT_TRUE(early.RunFor(4));  // delivered at 1, durable at 6
  EXPECT_EQ(early.Core(2)->Term(), 7u);
  early.Crash(2);
  EXPECT_FALSE(early.IsUp(2));
  early.Restart(2);
  EXPECT_EQ(early.Core(2)->Term(), 0u);

  RaftSimulator late(options);
  late.Send(Message(1, 2, 7));
  ASSERT_TRUE(late.RunFor(6));
  late.Crash(2);
  late.Restart(2);
  EXPECT_EQ(late.Core(2)->Term(), 7u);
  EXPECT_EQ(late.Log(2).HardState().term, 7u);
}

TEST(RaftSimulatorTest, RandomCrashesNeverTakeDownAMajority) {
  SimulationOptions options = Calm();
  options.crashPpm = 50'000;
  options.restartDelayMinTicks = 5;
  options.restartDelayMaxTicks = 20;
  RaftSimulator sim(options);
  sim.AddInvariant("majority up", [](const RaftSimulator &s) {
    std::size_t up = 0;
    for (const std::uint32_t id : s.Members()) up += s.IsUp(id) ? 1 : 0;
    return up >= 2 ? std::nullopt
                   : std::optional<std::string>(std::to_string(up) + " up");
  });
  ASSERT_TRUE(sim.RunFor(2000)) << sim.Report();
  EXPECT_GT(sim.Counters().crashes, 0u);
  EXPECT_GT(sim.Counters().restarts, 0u);
}

// The simulators live side by side so that nothing tied to an object's
// address can pass for determinism.
TEST(RaftSimulatorTest, SameSeedReplaysTheSameRun) {
  RaftSimulator first(Chaos(42));
  RaftSimulator second(Chaos(42));
  RaftSimulator other(Chaos(43));
  DriveChaos(first);
  DriveChaos(second);
  DriveChaos(other);
  EXPECT_EQ(first.Trace(), second.Trace());
  EXPECT_EQ(first.Digest(), second.Digest());
  EXPECT_NE(first.Digest(), other.Digest());
  // A run that exercised nothing proves nothing.
  EXPECT_GT(first.Counters().delivered, 0u);
  EXPECT_GT(first.Counters().dropped, 0u);
  EXPECT_GT(first.Counters().duplicated, 0u);
  EXPECT_GT(first.Counters().diskWrites, 0u);
  EXPECT_GT(first.Counters().crashes, 0u);
}

TEST(RaftSimulatorTest, DigestDoesNotDependOnTracing) {
  RaftSimulator traced(Chaos(7));
  RaftSimulator untraced(Untraced(Chaos(7)));
  DriveChaos(traced);
  DriveChaos(untraced);
  EXPECT_FALSE(traced.Trace().empty());
  EXPECT_TRUE(untraced.Trace().empty());
  EXPECT_EQ(traced.Digest(), untraced.Digest());
}

TEST(RaftSimulatorTest, RunDoesNotDependOnTheTraceWriter) {
  std::ostringstream out;
  RaftTraceWriter writer(out);
  SimulationOptions options = Chaos(7);
  options.traceWriter = &writer;
  RaftSimulator written(options);
  RaftSimulator plain(Chaos(7));
  DriveChaos(written);
  DriveChaos(plain);
  EXPECT_EQ(written.Trace(), plain.Trace());
  EXPECT_EQ(written.Digest(), plain.Digest());
  EXPECT_GT(writer.Lines(), written.Counters().delivered);
  EXPECT_EQ(out.str().rfind("{\"e\":\"init\",\"nodes\":[1,2,3]}\n", 0), 0u);
}

TEST(RaftSimulatorTest, ViolationStopsTheRunAndIsReported) {
  RaftSimulator sim(Calm(99));
  sim.AddInvariant("time limit", [](const RaftSimulator &s) {
    return s.Now() >= 5 ? std::optional<std::string>("reached") : std::nullopt;
  });
  EXPECT_FALSE(sim.RunFor(100));
  EXPECT_EQ(sim.Now(), 5u);
  ASSERT_TRUE(sim.Violation());
  EXPECT_EQ(*sim.Violation(), "time limit: reached");
  const std::string report = sim.Report();
  EXPECT_NE(report.find("seed=99"), std::string::npos);
  EXPECT_NE(report.find("violation: time limit: reached"), std::string::npos);
  EXPECT_NE(report.find("t=5 tick"), std::string::npos);
}

}  // namespace
}  // namespace stepraft
