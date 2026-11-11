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
#include "stepraft/cRaftMembership.hpp"
#include "stepraft/sRaftHardState.hpp"
#include "stepraft/sRaftLogEntry.hpp"

namespace stepraft {

// What a node writes before it starts again as its cluster's only voter,
// after the cluster lost its majority for good (etcd's --force-new-cluster).
// Unlike etcd it keeps its log: everything in it counts committed. The
// membership entry goes in a new term, so it never shares an index and
// a term with an entry of a member that comes back.
struct RaftSoleVoterStart {
  RaftLogEntry entry;
  RaftHardState hardState;
};

inline RaftSoleVoterStart SoleVoterStart(
    const RaftHardState &state, std::uint64_t lastIndex, std::uint32_t selfId,
    std::uint64_t storageId, std::vector<std::uint8_t> context = {}) {
  RaftSoleVoterStart start;
  const std::vector<RaftVoter> voters{{selfId, storageId}};
  start.entry.index = lastIndex + 1;
  start.entry.term = state.term + 1;
  start.entry.kind = RaftEntryKind::Membership;
  start.entry.payload = RaftMembership::Encode(voters, context);
  start.hardState.term = state.term + 1;
  start.hardState.votedFor = selfId;
  start.hardState.commitIndex = start.entry.index;
  start.hardState.voters = voters;
  start.hardState.votersIndex = start.entry.index;
  start.hardState.votersContext = std::move(context);
  return start;
}

}  // namespace stepraft
