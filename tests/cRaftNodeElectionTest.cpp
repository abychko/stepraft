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

#include "hRaftScenario.hpp"

namespace stepraft {
namespace {

using namespace scenario;

TEST(RaftNodeElectionTest, CalmRingElectsOneStableLeader) {
  for (std::uint64_t seed = 1; seed <= 20; ++seed) {
    for (const std::uint32_t nodes : {3u, 5u}) {
      RaftSimulator sim(Ring(seed, nodes));
      const auto leader = RunUntilLeader(sim, 100);
      ASSERT_TRUE(leader) << sim.Report();
      const std::uint64_t term = sim.Core(*leader)->Term();
      ASSERT_TRUE(sim.RunFor(300)) << sim.Report();
      EXPECT_EQ(AgreedLeader(sim), leader) << sim.Report();
      EXPECT_EQ(sim.Core(*leader)->Term(), term);

      // The leader opened its term with a no-op entry.
      const InMemoryRaftLog &log = sim.Log(*leader);
      ASSERT_GE(log.LastIndex(), 1u);
      const auto last = log.Read(log.LastIndex(), log.LastIndex(), 0);
      ASSERT_EQ(last.size(), 1u);
      EXPECT_EQ(last.front().kind, RaftEntryKind::NoOp);
      EXPECT_EQ(last.front().term, term);
    }
  }
}

TEST(RaftNodeElectionTest, LeaderCrashLeadsToANewLeaderInALaterTerm) {
  RaftSimulator sim(Ring(3));
  const auto first = RunUntilLeader(sim, 100);
  ASSERT_TRUE(first);
  const std::uint64_t firstTerm = sim.Core(*first)->Term();
  sim.Crash(*first);
  const auto second = RunUntilLeader(sim, 100);
  ASSERT_TRUE(second) << sim.Report();
  EXPECT_NE(*second, *first);
  EXPECT_GT(sim.Core(*second)->Term(), firstTerm);

  // The old leader comes back as a follower of the new one.
  sim.Restart(*first);
  ASSERT_TRUE(sim.RunFor(50));
  EXPECT_EQ(AgreedLeader(sim), second) << sim.Report();
}

TEST(RaftNodeElectionTest, IsolatedLeaderStepsDownAndTheMajorityMovesOn) {
  RaftSimulator sim(Ring(5));
  const auto old = RunUntilLeader(sim, 100);
  ASSERT_TRUE(old);
  const std::uint64_t oldTerm = sim.Core(*old)->Term();
  const std::vector<std::uint32_t> rest = AllBut(sim, *old);
  sim.Partition({{*old}, rest});

  ASSERT_TRUE(sim.RunFor(100)) << sim.Report();
  // Check-quorum: a leader that hears no majority gives up by itself.
  EXPECT_NE(sim.Core(*old)->Role(), RaftRole::Leader);
  const std::uint32_t witness = rest.front();
  const std::uint32_t newLeader = sim.Core(witness)->LeaderId();
  ASSERT_NE(newLeader, 0u) << sim.Report();
  EXPECT_NE(newLeader, *old);
  EXPECT_GT(sim.Core(newLeader)->Term(), oldTerm);

  sim.Heal();
  ASSERT_TRUE(sim.RunFor(50));
  EXPECT_EQ(AgreedLeader(sim), newLeader) << sim.Report();
}

// One follower loses only its link to the leader. With pre-vote and
// check-quorum it cannot win a pre-vote from members that still hear the
// leader, so it never bumps the term and the leader is never disturbed.
TEST(RaftNodeElectionTest, PartialPartitionDoesNotUnseatAStableLeader) {
  RaftSimulator sim(Ring(11));
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint64_t term = sim.Core(*leader)->Term();
  const std::uint32_t cutOff = OtherThan(sim, {*leader});
  sim.Cut(*leader, cutOff);
  for (int tick = 0; tick < 500; ++tick) {
    ASSERT_TRUE(sim.RunFor(1)) << sim.Report();
    ASSERT_EQ(sim.Core(*leader)->Role(), RaftRole::Leader) << sim.Report();
    ASSERT_EQ(sim.Core(*leader)->Term(), term);
    ASSERT_EQ(sim.Core(cutOff)->Term(), term);
  }
}

// The same partial partition without the pair: the cut-off follower
// starts a real election, wins the member that still hears the leader,
// and unseats a leader that was working. This is what the scenario above
// is there to rule out.
TEST(RaftNodeElectionTest, WithoutThePairAPartialPartitionUnseatsTheLeader) {
  SimulationOptions options = Ring(11);
  options.raft.preVoteAndCheckQuorum = false;
  RaftSimulator sim(options);
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint64_t term = sim.Core(*leader)->Term();
  const std::uint32_t cutOff = OtherThan(sim, {*leader});
  const std::uint32_t bystander = OtherThan(sim, {*leader, cutOff});
  sim.Cut(*leader, cutOff);
  ASSERT_TRUE(sim.RunFor(500)) << sim.Report();
  EXPECT_GT(sim.Core(cutOff)->Term(), term);
  EXPECT_GT(sim.Core(bystander)->Term(), term);
  EXPECT_NE(sim.Core(bystander)->LeaderId(), *leader);
}

TEST(RaftNodeElectionTest, RejoiningNodeDoesNotDisruptTheRing) {
  RaftSimulator sim(Ring(17));
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint64_t term = sim.Core(*leader)->Term();
  const std::uint32_t loner = OtherThan(sim, {*leader});
  sim.Partition({{loner}, AllBut(sim, loner)});
  ASSERT_TRUE(sim.RunFor(300));
  // Pre-vote: an isolated node probes, never wins, never bumps its term.
  EXPECT_EQ(sim.Core(loner)->Term(), term);
  sim.Heal();
  ASSERT_TRUE(sim.RunFor(100));
  EXPECT_EQ(AgreedLeader(sim), leader) << sim.Report();
  EXPECT_EQ(sim.Core(*leader)->Term(), term);
}

// A vote is granted, the voter crashes before the vote is on disk and
// restarts without it. Only a vote that never left the node may be cast
// again; the simulator checks every grant that leaves a node.
TEST(RaftNodeElectionTest, VoteIsOnDiskBeforeItIsSent) {
  SimulationOptions options = Ring(1);
  options.raft.electionTimeoutMinTicks = 1'000'000;
  options.raft.electionTimeoutMaxTicks = 1'000'000;
  options.diskDelayMinTicks = 5;
  options.diskDelayMaxTicks = 5;
  RaftSimulator sim(options);
  RaftMessage request;
  request.type = RaftMessageType::VoteRequest;
  request.from = 1;
  request.to = 3;
  request.term = 5;
  sim.Send(request);
  ASSERT_TRUE(sim.RunFor(2));  // granted at 1, durable at 6
  sim.Crash(3);
  sim.Restart(3);
  request.from = 2;
  sim.Send(request);
  ASSERT_TRUE(sim.RunFor(10)) << sim.Report();
  EXPECT_EQ(sim.Log(3).HardState().votedFor, 2u);
}

}  // namespace
}  // namespace stepraft
