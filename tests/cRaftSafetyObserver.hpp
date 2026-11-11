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
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "stepraft/sRaftRole.hpp"

namespace stepraft {

// Safety checks that need history across nodes and time. Fed one node
// snapshot at a time after every simulator event; returns a description
// of the first violation it sees.
class RaftSafetyObserver {
 public:
  // The node's view of its own log: the term at an index, if it has one.
  using TermLookup = std::function<std::optional<std::uint64_t>(std::uint64_t)>;

  std::optional<std::string> Observe(std::uint32_t node,
                                     std::uint64_t incarnation, RaftRole role,
                                     std::uint64_t term,
                                     std::uint64_t commitIndex,
                                     const TermLookup &termAt,
                                     std::uint64_t firstIndex = 1);
  // A vote grant as it leaves the voter. Across all lives of a voter, a
  // node on one storage, one term gets at most one candidate: a vote lost
  // in a crash after it was sent would let one term elect two leaders.
  std::optional<std::string> ObserveVoteGrant(std::uint32_t voter,
                                              std::uint64_t storageId,
                                              std::uint64_t term,
                                              std::uint32_t candidate);

  // Highest index any node has reported committed.
  std::uint64_t CommittedIndex() const { return committedTerm_.size(); }
  // A cluster that lost its majority goes on from a survivor: what others
  // committed past its log is given up.
  void ForgetCommitsAbove(std::uint64_t index) {
    if (committedTerm_.size() > index) committedTerm_.resize(index);
  }

 private:
  // Election safety: at most one leader per term, ever.
  std::map<std::uint64_t, std::uint32_t> leaderOfTerm_;
  // node -> (incarnation, highest commit index seen in it, committed
  // entries of it already compared). The commit index is volatile state,
  // so only a restart may lower it.
  std::map<std::uint32_t,
           std::tuple<std::uint64_t, std::uint64_t, std::uint64_t>>
      commit_;
  // Term of every committed entry, index 1 first: once committed, the
  // entry at an index is the same on every node (state machine safety),
  // and every later leader has it (leader completeness).
  std::vector<std::uint64_t> committedTerm_;
  // (term, voter) -> candidate.
  // (term, voter, its storage) -> candidate: a node back on other
  // storage is another voter.
  std::map<std::tuple<std::uint64_t, std::uint32_t, std::uint64_t>,
           std::uint32_t>
      grant_;
};

}  // namespace stepraft
