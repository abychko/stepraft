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

#include <algorithm>

namespace stepraft {

std::optional<std::string> RaftSafetyObserver::Observe(
    std::uint32_t node, std::uint64_t incarnation, RaftRole role,
    std::uint64_t term, std::uint64_t commitIndex, const TermLookup &termAt,
    std::uint64_t firstIndex) {
  const std::string who = "node " + std::to_string(node);
  // Below its base a node holds nothing to compare: it evicted or skipped
  // only what was committed.
  const std::uint64_t from = firstIndex > 1 ? firstIndex - 1 : 1;
  if (role == RaftRole::Leader) {
    const auto [it, inserted] = leaderOfTerm_.emplace(term, node);
    if (!inserted && it->second != node)
      return "election safety: term " + std::to_string(term) +
             " has two leaders, " + std::to_string(it->second) + " and " +
             std::to_string(node);
    if (inserted) {
      for (std::uint64_t index = from; index <= committedTerm_.size(); ++index)
        if (termAt(index) != committedTerm_[index - 1])
          return "leader completeness: " + who + ", leader of term " +
                 std::to_string(term) + ", lacks committed entry " +
                 std::to_string(index);
    }
  }

  auto &[seenIncarnation, seenCommit, compared] = commit_[node];
  if (seenIncarnation != incarnation) {
    seenIncarnation = incarnation;
    seenCommit = 0;
    compared = 0;
  }
  if (commitIndex < seenCommit)
    return "commit index of " + who + " went back from " +
           std::to_string(seenCommit) + " to " + std::to_string(commitIndex) +
           " without a restart";
  seenCommit = commitIndex;
  for (std::uint64_t index = std::max(compared + 1, from); index <= commitIndex;
       ++index) {
    const std::optional<std::uint64_t> have = termAt(index);
    if (!have)
      return who + " commits index " + std::to_string(index) +
             " beyond its log";
    if (index > committedTerm_.size() + 1)
      return who + " commits index " + std::to_string(index) +
             " while no node has committed index " +
             std::to_string(committedTerm_.size() + 1);
    if (index > committedTerm_.size()) {
      committedTerm_.push_back(*have);
    } else if (committedTerm_[index - 1] != *have) {
      return "state machine safety: index " + std::to_string(index) +
             " committed with term " +
             std::to_string(committedTerm_[index - 1]) + " and, on " + who +
             ", term " + std::to_string(*have);
    }
  }
  compared = commitIndex;
  return std::nullopt;
}

std::optional<std::string> RaftSafetyObserver::ObserveVoteGrant(
    std::uint32_t voter, std::uint64_t storageId, std::uint64_t term,
    std::uint32_t candidate) {
  const auto [it, inserted] =
      grant_.emplace(std::tuple(term, voter, storageId), candidate);
  if (!inserted && it->second != candidate)
    return "node " + std::to_string(voter) + " granted its term " +
           std::to_string(term) + " vote to both " +
           std::to_string(it->second) + " and " + std::to_string(candidate);
  return std::nullopt;
}

}  // namespace stepraft
