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
#include "hRaftScenario.hpp"

#include <gtest/gtest.h>

#include <functional>

namespace stepraft {
namespace {

using scenario::Payload;

SimulationOptions Pinned(std::uint64_t seed, std::uint32_t nodes = 3) {
  return scenario::Ring(seed, nodes);
}

bool RunUntil(RaftSimulator &sim, const std::function<bool()> &done,
              std::uint64_t maxTicks) {
  for (std::uint64_t tick = 0; tick < maxTicks; ++tick) {
    if (done()) return true;
    if (!sim.RunFor(1)) return false;
  }
  return done();
}

// Every running node holds `voters` as its membership.
bool AllHold(const RaftSimulator &sim, const std::vector<RaftVoter> &voters) {
  for (const std::uint32_t id : sim.Members())
    if (sim.IsUp(id) && sim.Core(id)->Voters() != voters) return false;
  return true;
}

std::vector<RaftVoter> Without(std::vector<RaftVoter> voters,
                               std::uint32_t id) {
  std::erase_if(voters, [id](const RaftVoter &v) { return v.id == id; });
  return voters;
}

std::uint32_t Leader(RaftSimulator &sim) {
  const auto leader = scenario::RunUntilLeader(sim, 500);
  EXPECT_TRUE(leader) << sim.Report();
  return leader.value_or(0);
}

// Proposes until the leader takes one, then until it commits there.
void Commit(RaftSimulator &sim, std::uint32_t leader, std::uint64_t value) {
  ASSERT_TRUE(sim.Propose(leader, Payload(value))) << sim.Report();
  const std::uint64_t index = sim.Core(leader)->LastIndex();
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(leader)->CommitIndex() >= index; }, 500))
      << sim.Report();
}

TEST(RaftMembershipTest, AWipedNodeFollowsTheLogWithoutAVote) {
  RaftSimulator sim(Pinned(11));
  sim.AddInvariant(
      "a wiped node never campaigns",
      [](const RaftSimulator &s) -> std::optional<std::string> {
        for (const std::uint32_t id : s.Members()) {
          const RaftNode *core = s.Core(id);
          if (core && !core->IsVoter() && core->Role() != RaftRole::Follower)
            return "node " + std::to_string(id) + " campaigns without a vote";
        }
        return std::nullopt;
      });
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t wiped = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, wiped});
  sim.Wipe(wiped);
  sim.Restart(wiped);
  Commit(sim, leader, 2);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(wiped)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  EXPECT_FALSE(sim.Core(wiped)->IsVoter());
  // With the other voter gone, the leader and the wiped node are no
  // majority: the leader steps down and nobody is elected.
  sim.Crash(other);
  ASSERT_TRUE(sim.RunFor(400)) << sim.Report();
  for (const std::uint32_t id : {leader, wiped})
    EXPECT_NE(sim.Core(id)->Role(), RaftRole::Leader) << "node " << id;
  sim.Restart(other);
  EXPECT_NE(Leader(sim), wiped);
}

TEST(RaftMembershipTest, AWipedMemberIsReplacedRemovedFirstThenAdded) {
  RaftSimulator sim(Pinned(12));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t wiped = scenario::OtherThan(sim, {leader});
  const std::uint64_t old = sim.StorageOf(wiped);
  sim.Wipe(wiped);
  sim.Restart(wiped);
  const RaftVoter fresh{wiped, sim.StorageOf(wiped)};
  ASSERT_NE(fresh.storageId, old);
  const std::vector<RaftVoter> start = sim.Core(leader)->Voters();

  // Its node already votes with the old storage.
  EXPECT_EQ(
      sim.ChangeMembership(leader, {RaftMembershipChangeKind::AddVoter, fresh}),
      RaftMembershipOutcome::Invalid);
  EXPECT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::RemoveVoter, {wiped, old}}),
            RaftMembershipOutcome::Accepted);
  // One change at a time.
  EXPECT_EQ(
      sim.ChangeMembership(leader, {RaftMembershipChangeKind::AddVoter, fresh}),
      RaftMembershipOutcome::Busy);
  const std::vector<RaftVoter> two = Without(start, wiped);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllHold(sim, two); }, 500))
      << sim.Report();

  RaftMembershipOutcome added = RaftMembershipOutcome::Busy;
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        added = sim.ChangeMembership(
            leader, {RaftMembershipChangeKind::AddVoter, fresh});
        return added == RaftMembershipOutcome::Accepted;
      },
      500))
      << "last outcome " << static_cast<int>(added) << "\n"
      << sim.Report();
  std::vector<RaftVoter> three = two;
  three.push_back(fresh);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllHold(sim, three); }, 500))
      << sim.Report();
  EXPECT_TRUE(sim.Core(wiped)->IsVoter());
  Commit(sim, leader, 2);

  // Its vote counts again: without the leader, the other two elect one.
  sim.Crash(leader);
  const std::uint32_t next = Leader(sim);
  EXPECT_NE(next, leader);
  Commit(sim, next, 3);
}

TEST(RaftMembershipTest, ANewLeaderChangesNothingBeforeAnEntryOfItsTerm) {
  RaftSimulator sim(Pinned(13, 5));
  std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t wiped = scenario::OtherThan(sim, {leader});
  const std::uint64_t old = sim.StorageOf(wiped);
  sim.Wipe(wiped);
  sim.Restart(wiped);
  ASSERT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::RemoveVoter, {wiped, old}}),
            RaftMembershipOutcome::Accepted);
  const std::vector<RaftVoter> four =
      Without(sim.Core(leader)->Voters(), wiped);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllHold(sim, four); }, 500))
      << sim.Report();

  // The leader goes between the two changes; the next one adds nothing
  // until an entry of its own term commits.
  const std::uint64_t term = sim.Core(leader)->Term();
  sim.Crash(leader);
  std::uint32_t next = 0;
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        for (const std::uint32_t id : sim.Members()) {
          const RaftNode *core = sim.Core(id);
          if (core && core->Role() == RaftRole::Leader && core->Term() > term)
            next = id;
        }
        return next != 0;
      },
      500))
      << sim.Report();
  const RaftNode &core = *sim.Core(next);
  ASSERT_NE(core.TermAt(core.CommitIndex()), core.Term());
  const RaftVoter fresh{wiped, sim.StorageOf(wiped)};
  EXPECT_EQ(
      sim.ChangeMembership(next, {RaftMembershipChangeKind::AddVoter, fresh}),
      RaftMembershipOutcome::Busy);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.ChangeMembership(
                   next, {RaftMembershipChangeKind::AddVoter, fresh}) ==
               RaftMembershipOutcome::Accepted;
      },
      500))
      << sim.Report();
  std::vector<RaftVoter> five = four;
  five.push_back(fresh);
  sim.Restart(leader);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllHold(sim, five); }, 500))
      << sim.Report();
}

TEST(RaftMembershipTest, ALeaderTheMembershipLeavesOutStepsDown) {
  RaftSimulator sim(Pinned(14));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const RaftVoter self{leader, sim.StorageOf(leader)};
  ASSERT_EQ(sim.ChangeMembership(leader,
                                 {RaftMembershipChangeKind::RemoveVoter, self}),
            RaftMembershipOutcome::Accepted);
  const std::vector<RaftVoter> rest =
      Without(sim.Core(leader)->Voters(), leader);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllHold(sim, rest); }, 500))
      << sim.Report();
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(leader)->Role() != RaftRole::Leader; }, 500))
      << sim.Report();
  const std::uint32_t next = Leader(sim);
  EXPECT_NE(next, leader);
  EXPECT_FALSE(sim.Core(leader)->IsVoter());
  Commit(sim, next, 2);
}

TEST(RaftMembershipTest, AMembershipEntryCutFromTheLogTakesItsMembershipAlong) {
  RaftSimulator sim(Pinned(16));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::vector<RaftVoter> start = sim.Core(leader)->Voters();
  const std::uint32_t follower = scenario::OtherThan(sim, {leader});
  sim.Partition({{leader}, scenario::AllBut(sim, leader)});
  ASSERT_EQ(sim.ChangeMembership(leader, {RaftMembershipChangeKind::RemoveVoter,
                                          {follower, sim.StorageOf(follower)}}),
            RaftMembershipOutcome::Accepted);
  EXPECT_EQ(sim.Core(leader)->Voters(), Without(start, follower));
  // The other two elect a leader of their own; back, the old one has its
  // entry cut.
  std::uint32_t next = 0;
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        for (const std::uint32_t id : scenario::AllBut(sim, leader))
          if (sim.Core(id)->Role() == RaftRole::Leader) next = id;
        return next != 0;
      },
      500))
      << sim.Report();
  Commit(sim, next, 2);
  sim.Heal();
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(leader)->CommitIndex() >= sim.Core(next)->CommitIndex();
      },
      500))
      << sim.Report();
  EXPECT_TRUE(AllHold(sim, start));
}

TEST(RaftStartAtTest, AFollowerBehindTheLeadersLogStartRestartsItsOwn) {
  RaftSimulator sim(scenario::Ring(17));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t away = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, away});
  Commit(sim, leader, 1);
  sim.Crash(away);
  for (std::uint64_t value = 2; value <= 20; ++value)
    Commit(sim, leader, value);
  ASSERT_TRUE(sim.RunFor(10)) << sim.Report();
  const std::uint64_t cut = sim.Core(leader)->CommitIndex() - 5;
  ASSERT_TRUE(sim.Evict(leader, cut));
  ASSERT_TRUE(sim.Evict(other, cut));
  ASSERT_LT(sim.Log(away).LastIndex(), cut);
  sim.Restart(away);
  Commit(sim, leader, 21);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(away)->CommitIndex() ==
                   sim.Core(leader)->CommitIndex() &&
               sim.Core(away)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  EXPECT_GE(sim.Counters().resets, 1u);
  EXPECT_EQ(sim.Log(away).FirstIndex(), cut + 1);
  // It is a voter as before: with it, the cluster survives the other's loss.
  sim.Crash(other);
  Commit(sim, leader, 22);
}

// The leader's own membership steps, taken each tick, until `done`.
bool Steps(RaftSimulator &sim, const std::function<bool()> &done,
           std::uint64_t maxTicks) {
  for (std::uint64_t tick = 0; tick < maxTicks; ++tick) {
    if (done()) return true;
    if (const auto leader = scenario::AgreedLeader(sim))
      if (const auto step = sim.Core(*leader)->MembershipStep())
        sim.ChangeMembership(*leader, *step);
    if (!sim.RunFor(1)) return false;
  }
  return done();
}

bool Pinned(const RaftSimulator &sim, std::uint32_t leader) {
  for (const RaftVoter &voter : sim.Core(leader)->Voters())
    if (voter.storageId != sim.StorageOf(voter.id)) return false;
  return sim.Core(leader)->Voters().size() == sim.Members().size();
}

// Two members of five back on new storage: one is out of the voters at a
// time, and the second goes while the other four still carry the cluster.
TEST(RaftMembershipStepTest, TwoWipedMembersAreReplacedOneAtATime) {
  RaftSimulator sim(Pinned(33, 5));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t first = scenario::OtherThan(sim, {leader});
  const std::uint32_t second = scenario::OtherThan(sim, {leader, first});
  for (const std::uint32_t id : {first, second}) {
    sim.Wipe(id);
    sim.Restart(id);
  }
  std::size_t fewest = sim.Members().size();
  ASSERT_TRUE(Steps(
      sim,
      [&] {
        fewest = std::min(fewest, sim.Core(leader)->Voters().size());
        return Pinned(sim, leader);
      },
      2000))
      << sim.Report();
  EXPECT_EQ(fewest, sim.Members().size() - 1);
  Commit(sim, leader, 2);
}

// A follower comes back on new storage: the leader removes its old voter
// and, once it caught up, adds it on the new storage. Nothing else steps
// in, and afterwards the cluster survives the other follower's loss.
TEST(RaftMembershipStepTest, AWipedMemberIsReplacedByTheLeaderAlone) {
  RaftSimulator sim(Pinned(23));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t wiped = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, wiped});
  for (std::uint64_t value = 1; value <= 10; ++value)
    Commit(sim, leader, value);
  const std::uint64_t old = sim.StorageOf(wiped);
  sim.Wipe(wiped);
  sim.Restart(wiped);
  const RaftVoter fresh{wiped, sim.StorageOf(wiped)};
  ASSERT_NE(fresh.storageId, old);
  ASSERT_TRUE(Steps(
      sim,
      [&] {
        const std::vector<RaftVoter> &voters = sim.Core(leader)->Voters();
        return std::find(voters.begin(), voters.end(), fresh) != voters.end();
      },
      500))
      << sim.Report();
  ASSERT_TRUE(Steps(
      sim, [&] { return sim.Core(wiped)->IsVoter(); }, 500))
      << sim.Report();
  EXPECT_FALSE(sim.Core(leader)->MembershipStep());
  sim.Crash(other);
  Commit(sim, leader, 11);
}

// A wiped node, on a new storage, has nothing at all: index 0 has a term
// on any log, and still the leader has no entry to send after it.
TEST(RaftStartAtTest, AnEmptyLogStartsAfterTheLeadersBase) {
  RaftSimulator sim(Pinned(19));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t away = scenario::OtherThan(sim, {leader});
  for (std::uint64_t value = 1; value <= 20; ++value)
    Commit(sim, leader, value);
  ASSERT_TRUE(sim.RunFor(10)) << sim.Report();
  const std::uint64_t cut = sim.Core(leader)->CommitIndex() - 5;
  for (const std::uint32_t id : sim.Members()) {
    if (id != away) {
      ASSERT_TRUE(sim.Evict(id, cut));
    }
  }
  sim.Wipe(away);
  sim.Restart(away);
  ASSERT_EQ(sim.Log(away).LastIndex(), 0u);
  Commit(sim, leader, 21);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(away)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  EXPECT_EQ(sim.Log(away).FirstIndex(), cut + 1);
  EXPECT_EQ(sim.Log(away).TermAt(cut), sim.Log(leader).TermAt(cut));
}

// A node an entry not yet committed adds back may be the only one that
// can win; elected, it leads, and does not step down for the committed
// membership that still leaves it out (Ongaro 4.2.2: a leader leaves once
// the membership without it commits).
TEST(RaftMembershipTest, ALeaderAnUncommittedEntryAddsBackKeepsLeading) {
  RaftSimulator sim(Pinned(29));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t back = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, back});
  Commit(sim, leader, 1);
  ASSERT_EQ(sim.ChangeMembership(leader, {RaftMembershipChangeKind::RemoveVoter,
                                          {back, sim.StorageOf(back)}}),
            RaftMembershipOutcome::Accepted);
  Commit(sim, leader, 2);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(back)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  // The entry that adds it back reaches it alone.
  sim.Cut(leader, other);
  ASSERT_EQ(sim.ChangeMembership(leader, {RaftMembershipChangeKind::AddVoter,
                                          {back, sim.StorageOf(back)}}),
            RaftMembershipOutcome::Accepted);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(back)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  sim.Crash(leader);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        const RaftNode &core = *sim.Core(back);
        return core.Role() == RaftRole::Leader &&
               core.CommitIndex() == core.LastIndex();
      },
      1000))
      << sim.Report();
}

// A cluster left with one voter elects it at once: no answer will come.
TEST(RaftMembershipTest, TheOnlyVoterElectsItself) {
  RaftSimulator sim(Pinned(31));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  for (const std::uint32_t id : sim.Members()) {
    if (id == leader) continue;
    ASSERT_EQ(
        sim.ChangeMembership(leader, {RaftMembershipChangeKind::RemoveVoter,
                                      {id, sim.StorageOf(id)}}),
        RaftMembershipOutcome::Accepted);
    Commit(sim, leader, 2 + id);
  }
  ASSERT_EQ(sim.Core(leader)->Voters().size(), 1u);
  sim.Crash(leader);
  sim.Restart(leader);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(leader)->Role() == RaftRole::Leader; }, 500))
      << sim.Report();
}

// A cluster of one node takes two more, one at a time: each is first made
// known to the members, follows the log without a vote, and is added as a
// voter once it holds what is committed.
TEST(RaftMembershipTest, ARingOfOneGrowsToThree) {
  SimulationOptions options = Pinned(37);
  options.known = {1};
  options.voters = {1};
  RaftSimulator sim(options);
  sim.Crash(2);
  sim.Crash(3);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(1)->Role() == RaftRole::Leader; }, 500))
      << sim.Report();
  Commit(sim, 1, 1);
  EXPECT_FALSE(sim.Core(1)->MembershipStep());
  std::vector<std::uint32_t> members{1};
  std::vector<RaftVoter> voters{{1, sim.StorageOf(1)}};
  for (const std::uint32_t joiner : {2u, 3u}) {
    members.push_back(joiner);
    for (const std::uint32_t id : members)
      if (id != joiner) sim.SetMembers(id, members);
    sim.Restart(joiner);
    const auto leader = scenario::RunUntilLeader(sim, 500);
    ASSERT_TRUE(leader) << sim.Report();
    ASSERT_NE(*leader, joiner);
    std::optional<RaftMembershipChange> step;
    ASSERT_TRUE(RunUntil(
        sim,
        [&] {
          step = sim.Core(*leader)->MembershipStep();
          return step.has_value();
        },
        500))
        << sim.Report();
    EXPECT_EQ(step->kind, RaftMembershipChangeKind::AddVoter);
    EXPECT_EQ(step->voter, (RaftVoter{joiner, sim.StorageOf(joiner)}));
    ASSERT_EQ(sim.ChangeMembership(*leader, *step),
              RaftMembershipOutcome::Accepted);
    voters.push_back(step->voter);
    ASSERT_TRUE(RunUntil(
        sim, [&] { return AllHold(sim, voters); }, 500))
        << sim.Report();
    Commit(sim, *leader, 10 + joiner);
    EXPECT_FALSE(sim.Core(*leader)->MembershipStep());
  }
  // Any two of the three carry the cluster now.
  const std::uint32_t leader = Leader(sim);
  sim.Crash(leader);
  const auto next = scenario::RunUntilLeader(sim, 1000);
  ASSERT_TRUE(next) << sim.Report();
  Commit(sim, *next, 99);
}

// The cluster loses two of three for good; the survivor, told to, goes on as
// the only voter and commits again. The other two come back with what they
// held, the old leader with entries the survivor never had: both follow,
// drop what conflicts, and the leader adds them back as voters.
TEST(RaftSoleVoterTest, TheSurvivorGoesOnAloneAndTakesTheOthersBack) {
  RaftSimulator sim(Pinned(35));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t survivor = scenario::OtherThan(sim, {leader});
  const std::uint32_t lost = scenario::OtherThan(sim, {leader, survivor});
  for (std::uint64_t value = 1; value <= 5; ++value) Commit(sim, leader, value);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(survivor)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  sim.Crash(lost);
  // The leader takes one more entry no one else gets, then goes too.
  sim.Cut(leader, survivor);
  ASSERT_TRUE(sim.Propose(leader, Payload(6)));
  ASSERT_TRUE(sim.RunFor(10)) << sim.Report();
  sim.Crash(leader);
  ASSERT_TRUE(sim.RunFor(300)) << sim.Report();
  EXPECT_NE(sim.Core(survivor)->Role(), RaftRole::Leader);

  sim.Crash(survivor);
  const std::uint64_t forced = sim.Log(survivor).LastIndex() + 1;
  sim.ContinueAlone(survivor);
  sim.Heal();
  sim.Restart(survivor);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(survivor)->Role() == RaftRole::Leader; }, 500))
      << sim.Report();
  ASSERT_EQ(sim.Core(survivor)->Voters().size(), 1u);
  Commit(sim, survivor, 7);

  sim.Restart(leader);
  sim.Restart(lost);
  ASSERT_TRUE(Steps(
      sim,
      [&] {
        return sim.Core(survivor)->Voters().size() == 3 &&
               sim.Core(leader)->IsVoter() && sim.Core(lost)->IsVoter();
      },
      2000))
      << sim.Report();
  Commit(sim, survivor, 8);
  EXPECT_TRUE(scenario::LogsIdentical(sim)) << scenario::DescribeNodes(sim);
  // The old leader's entry there is gone, not taken for the survivor's.
  for (const std::uint32_t id : sim.Members()) {
    const std::vector<RaftLogEntry> entry =
        sim.Log(id).Read(forced, forced, 1 << 20);
    ASSERT_EQ(entry.size(), 1u) << id;
    EXPECT_EQ(entry[0].kind, RaftEntryKind::Membership) << id;
  }
}

// Ongaro's counterexample to single-server changes without the rule
// (raft-dev, 2015): S1 adds S5 and reaches only S5; S2 is elected in the
// old membership and removes S4 at once, committing that with S3; S1
// comes back and wins with S4 and S5 — and would overwrite what S2
// committed. With the rule S2 cannot change the membership before an
// entry of its term commits, and it cannot commit one without S1 or S4.
TEST(RaftMembershipTest, OngarosCounterexampleCommitsNothingTwice) {
  SimulationOptions options = Pinned(15, 5);
  options.voters = {1, 2, 3, 4};
  RaftSimulator sim(options);
  std::uint32_t leader = Leader(sim);
  if (leader != 1) {
    sim.TransferLeadership(leader, 1);
    ASSERT_TRUE(RunUntil(
        sim,
        [&] {
          const auto agreed = scenario::AgreedLeader(sim);
          return agreed && *agreed == 1;
        },
        500))
        << sim.Report();
  }
  Commit(sim, 1, 1);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(5)->LastIndex() == sim.Core(1)->LastIndex(); },
      500))
      << sim.Report();

  // S1 adds S5; only S5 hears of it.
  for (const std::uint32_t id : {2u, 3u, 4u}) sim.Cut(1, id);
  ASSERT_EQ(sim.ChangeMembership(
                1, {RaftMembershipChangeKind::AddVoter, {5, sim.StorageOf(5)}}),
            RaftMembershipOutcome::Accepted);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(5)->LastIndex() == sim.Core(1)->LastIndex(); },
      100))
      << sim.Report();
  sim.Crash(1);

  // S2, S3 and S4 elect one of S2 and S3; S4 hears nothing of its term,
  // and S5 nothing at all.
  for (const std::uint32_t id : {2u, 3u, 4u}) sim.Cut(5, id);
  std::uint32_t second = 0;
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        for (const std::uint32_t id : {2u, 3u})
          if (sim.Core(id)->Role() == RaftRole::Leader) second = id;
        if (sim.Core(4)->Role() == RaftRole::Leader) {
          sim.TransferLeadership(4, 2);
        }
        return second != 0;
      },
      1000))
      << sim.Report();
  sim.Cut(4, 2);
  sim.Cut(4, 3);
  const RaftMembershipOutcome removed = sim.ChangeMembership(
      second, {RaftMembershipChangeKind::RemoveVoter, {4, sim.StorageOf(4)}});
  EXPECT_EQ(removed, RaftMembershipOutcome::Busy);
  ASSERT_TRUE(sim.RunFor(50)) << sim.Report();

  // S1 comes back to S4 and S5 alone, then the network heals.
  sim.Restart(1);
  sim.Mend(1, 4);
  sim.Mend(4, 5);
  ASSERT_TRUE(sim.RunFor(400)) << sim.Report();
  sim.Heal();
  ASSERT_TRUE(sim.RunFor(400)) << sim.Report();
  const std::uint32_t last = Leader(sim);
  Commit(sim, last, 2);
}

TEST(RaftMembershipContextTest,
     AMembershipEntryCarriesTheContextAfterTheVoters) {
  const std::vector<RaftVoter> voters{{1, 100}, {2, 200}};
  const std::vector<std::uint8_t> context{7, 8, 9};
  const std::vector<std::uint8_t> bare = RaftMembership::Encode(voters);
  const std::vector<std::uint8_t> with =
      RaftMembership::Encode(voters, context);
  EXPECT_EQ(RaftMembership::Decode(bare), voters);
  EXPECT_TRUE(RaftMembership::Context(bare).empty());
  EXPECT_EQ(RaftMembership::Decode(with), voters);
  EXPECT_EQ(RaftMembership::Context(with), context);
  EXPECT_EQ(with.size(), bare.size() + context.size());
  // Cut inside the voters: no membership.
  const std::span<const std::uint8_t> cut(bare.data(), bare.size() - 1);
  EXPECT_FALSE(RaftMembership::Decode(cut));
  EXPECT_TRUE(RaftMembership::Context(cut).empty());
}

// Every running node holds `context` as committed.
bool AllCarry(const RaftSimulator &sim,
              const std::vector<std::uint8_t> &context) {
  for (const std::uint32_t id : sim.Members())
    if (sim.IsUp(id) && sim.Core(id)->Context() != context) return false;
  return true;
}

TEST(RaftMembershipContextTest,
     TheContextCommitsEverywhereAndOutlivesARestartAndAChangeOfVoters) {
  RaftSimulator sim(Pinned(21));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t follower = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, follower});
  const std::vector<RaftVoter> start = sim.Core(leader)->Voters();
  const std::vector<std::uint8_t> context{7, 8, 9};

  EXPECT_EQ(sim.ChangeMembership(
                follower, {RaftMembershipChangeKind::SetContext, {}, context}),
            RaftMembershipOutcome::NotLeader);
  ASSERT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::SetContext, {}, context}),
            RaftMembershipOutcome::Accepted);
  EXPECT_EQ(sim.Core(leader)->Voters(), start);
  // Not committed yet.
  EXPECT_TRUE(sim.Core(leader)->Context().empty());
  EXPECT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::SetContext, {}, {1}}),
            RaftMembershipOutcome::Busy);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllCarry(sim, context); }, 500))
      << sim.Report();
  EXPECT_TRUE(AllHold(sim, start));

  sim.Crash(follower);
  sim.Restart(follower);
  Commit(sim, leader, 2);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return AllCarry(sim, context); }, 500))
      << sim.Report();

  // A change of the voters keeps it.
  const RaftVoter gone{other, sim.StorageOf(other)};
  ASSERT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::RemoveVoter, gone, {}}),
            RaftMembershipOutcome::Accepted);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(leader)->CommitIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  EXPECT_EQ(sim.Core(leader)->Voters(), Without(start, other));
  EXPECT_EQ(sim.Core(leader)->Context(), context);
  EXPECT_EQ(sim.Core(follower)->Context(), context);
}

TEST(RaftMembershipContextTest, AVoterGoesWithTheContextItsEntryCarries) {
  RaftSimulator sim(Pinned(25));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t follower = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, follower});
  const std::vector<RaftVoter> start = sim.Core(leader)->Voters();
  const std::vector<std::uint8_t> context{4, 5};
  const RaftVoter gone{other, sim.StorageOf(other)};
  ASSERT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::RemoveVoter, gone, context}),
            RaftMembershipOutcome::Accepted);
  // One entry: the voter and the context change together.
  EXPECT_EQ(sim.Core(leader)->Voters(), Without(start, other));
  EXPECT_EQ(sim.Core(leader)->LatestContext(), context);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return sim.Core(follower)->Context() == context; }, 500))
      << sim.Report();
  EXPECT_EQ(sim.Core(follower)->Voters(), Without(start, other));
}

TEST(RaftMembershipContextTest, AVoterGoesOnlyIfTheOthersCarryTheRing) {
  RaftSimulator sim(Pinned(27));
  const std::uint32_t leader = Leader(sim);
  Commit(sim, leader, 1);
  const std::uint32_t follower = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, follower});
  const RaftVoter up{follower, sim.StorageOf(follower)};
  const RaftVoter down{other, sim.StorageOf(other)};
  EXPECT_TRUE(sim.Core(leader)->CarriedWithout(up));
  EXPECT_FALSE(sim.Core(follower)->CarriedWithout(down));
  sim.Crash(other);
  ASSERT_TRUE(RunUntil(
      sim, [&] { return !sim.Core(leader)->CarriedWithout(up); }, 2000))
      << sim.Report();
  // Those that answered within the last election timeout count.
  EXPECT_TRUE(RunUntil(
      sim, [&] { return sim.Core(leader)->CarriedWithout(down); }, 2000))
      << sim.Report();
  EXPECT_FALSE(sim.Core(leader)->CarriedWithout(up));
}

TEST(RaftStartAtTest, AFollowerThatStartsOverTakesTheContext) {
  RaftSimulator sim(scenario::Ring(23));
  const std::uint32_t leader = Leader(sim);
  const std::uint32_t away = scenario::OtherThan(sim, {leader});
  const std::uint32_t other = scenario::OtherThan(sim, {leader, away});
  Commit(sim, leader, 1);
  sim.Crash(away);
  const std::vector<std::uint8_t> context{4, 2};
  ASSERT_EQ(sim.ChangeMembership(
                leader, {RaftMembershipChangeKind::SetContext, {}, context}),
            RaftMembershipOutcome::Accepted);
  for (std::uint64_t value = 2; value <= 20; ++value)
    Commit(sim, leader, value);
  ASSERT_TRUE(sim.RunFor(10)) << sim.Report();
  ASSERT_EQ(sim.Core(leader)->Context(), context);
  const std::uint64_t cut = sim.Core(leader)->CommitIndex() - 5;
  ASSERT_TRUE(sim.Evict(leader, cut));
  ASSERT_TRUE(sim.Evict(other, cut));
  sim.Restart(away);
  EXPECT_TRUE(sim.Core(away)->Context().empty());
  Commit(sim, leader, 21);
  ASSERT_TRUE(RunUntil(
      sim,
      [&] {
        return sim.Core(away)->LastIndex() == sim.Core(leader)->LastIndex();
      },
      500))
      << sim.Report();
  EXPECT_GE(sim.Counters().resets, 1u);
  EXPECT_EQ(sim.Core(away)->Context(), context);
  // What it wrote as it started over holds it too.
  sim.Crash(away);
  sim.Restart(away);
  EXPECT_EQ(sim.Core(away)->Context(), context);
}

}  // namespace
}  // namespace stepraft
