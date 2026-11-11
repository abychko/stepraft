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

#include "cRaftSafetyObserver.hpp"

#include <gtest/gtest.h>

namespace stepraft {
namespace {

// A log given as the term of each index, index 1 first.
RaftSafetyObserver::TermLookup Log(std::vector<std::uint64_t> terms) {
  return [terms = std::move(terms)](
             std::uint64_t index) -> std::optional<std::uint64_t> {
    if (index == 0) return 0;
    if (index > terms.size()) return std::nullopt;
    return terms[index - 1];
  };
}

TEST(RaftSafetyObserverTest, OneLeaderPerTermPasses) {
  RaftSafetyObserver observer;
  const auto log = Log({});
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Leader, 3, 0, log));
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Leader, 3, 0, log));
  EXPECT_FALSE(observer.Observe(2, 0, RaftRole::Follower, 3, 0, log));
  EXPECT_FALSE(observer.Observe(2, 0, RaftRole::Leader, 4, 0, log));
}

TEST(RaftSafetyObserverTest, TwoLeadersInOneTermAreCaught) {
  RaftSafetyObserver observer;
  const auto log = Log({});
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Leader, 3, 0, log));
  // Even after node 1 stepped down: the term already had a leader.
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Follower, 4, 0, log));
  const auto broken = observer.Observe(2, 0, RaftRole::Leader, 3, 0, log);
  ASSERT_TRUE(broken);
  EXPECT_NE(broken->find("term 3"), std::string::npos);
}

TEST(RaftSafetyObserverTest, CommitGoesBackOnlyAcrossRestart) {
  RaftSafetyObserver observer;
  const auto log = Log({1, 1, 1, 1, 1, 1, 1});
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Follower, 1, 5, log));
  EXPECT_FALSE(observer.Observe(1, 0, RaftRole::Follower, 1, 7, log));
  EXPECT_TRUE(observer.Observe(1, 0, RaftRole::Follower, 1, 6, log));

  RaftSafetyObserver restarted;
  EXPECT_FALSE(restarted.Observe(1, 0, RaftRole::Follower, 1, 7, log));
  EXPECT_FALSE(restarted.Observe(1, 1, RaftRole::Follower, 1, 2, log));
}

TEST(RaftSafetyObserverTest, ANodeIsComparedFromItsBaseOn) {
  RaftSafetyObserver observer;
  EXPECT_FALSE(
      observer.Observe(1, 0, RaftRole::Leader, 2, 5, Log({1, 1, 2, 2, 2})));
  // Node 2 starts after base 4: nothing below it to compare.
  const auto afterBase =
      [](std::uint64_t index) -> std::optional<std::uint64_t> {
    if (index == 4 || index == 5) return 2;
    return std::nullopt;
  };
  EXPECT_FALSE(observer.Observe(2, 0, RaftRole::Follower, 2, 5, afterBase, 5));
  EXPECT_FALSE(observer.Observe(2, 0, RaftRole::Leader, 3, 5, afterBase, 5));
  const auto wrongBase =
      [](std::uint64_t index) -> std::optional<std::uint64_t> {
    if (index == 4) return 1;
    return std::nullopt;
  };
  EXPECT_TRUE(observer.Observe(3, 0, RaftRole::Follower, 2, 4, wrongBase, 5));
}

TEST(RaftSafetyObserverTest, CommittedEntriesAgreeAcrossNodes) {
  RaftSafetyObserver observer;
  EXPECT_FALSE(
      observer.Observe(1, 0, RaftRole::Follower, 2, 3, Log({1, 1, 2})));
  EXPECT_EQ(observer.CommittedIndex(), 3u);
  EXPECT_FALSE(
      observer.Observe(2, 0, RaftRole::Follower, 2, 2, Log({1, 1, 3})));
  const auto broken =
      observer.Observe(2, 0, RaftRole::Follower, 3, 3, Log({1, 1, 3}));
  ASSERT_TRUE(broken);
  EXPECT_NE(broken->find("state machine safety: index 3"), std::string::npos);
  EXPECT_TRUE(observer.Observe(3, 0, RaftRole::Follower, 2, 4, Log({1})));
}

TEST(RaftSafetyObserverTest, NewLeaderMustHoldEverythingCommitted) {
  RaftSafetyObserver observer;
  EXPECT_FALSE(
      observer.Observe(1, 0, RaftRole::Follower, 2, 3, Log({1, 2, 2})));
  EXPECT_FALSE(
      observer.Observe(2, 0, RaftRole::Leader, 3, 0, Log({1, 2, 2, 3})));
  const auto broken =
      observer.Observe(3, 0, RaftRole::Leader, 4, 0, Log({1, 2}));
  ASSERT_TRUE(broken);
  EXPECT_NE(broken->find("leader completeness"), std::string::npos);
}

TEST(RaftSafetyObserverTest, OneVotePerTermAcrossRestarts) {
  RaftSafetyObserver observer;
  EXPECT_FALSE(observer.ObserveVoteGrant(3, 30, 5, 1));
  EXPECT_FALSE(observer.ObserveVoteGrant(3, 30, 5, 1));  // a duplicate
  EXPECT_FALSE(observer.ObserveVoteGrant(3, 30, 6, 2));  // a new term
  EXPECT_FALSE(observer.ObserveVoteGrant(2, 20, 5, 2));  // another voter
  EXPECT_FALSE(observer.ObserveVoteGrant(3, 31, 5, 2));  // other storage
  const auto broken = observer.ObserveVoteGrant(3, 30, 5, 2);
  ASSERT_TRUE(broken);
  EXPECT_NE(broken->find("term 5"), std::string::npos);
}

}  // namespace
}  // namespace stepraft
