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
#include <span>
#include <vector>

namespace stepraft {

// A voting member: the node and the storage its log lives on. A node
// that comes back with other storage is not this voter (KIP-853).
struct RaftVoter {
  std::uint32_t id = 0;
  std::uint64_t storageId = 0;
  bool operator==(const RaftVoter &) const = default;
};

// The voting members, as the core keeps them and as a membership entry
// carries them.
class RaftMembership {
 public:
  static bool Contains(const std::vector<RaftVoter> &voters, std::uint32_t id,
                       std::uint64_t storageId);
  static bool HasNode(const std::vector<RaftVoter> &voters, std::uint32_t id);
  static std::size_t Quorum(const std::vector<RaftVoter> &voters) {
    return voters.size() / 2 + 1;
  }
  // Count, then id and storage of each, little endian; then the context,
  // the rest of the bytes: what the cluster keeps with its membership, which
  // the core carries and never reads (etcd's ConfChange.Context).
  static std::vector<std::uint8_t> Encode(
      const std::vector<RaftVoter> &voters,
      std::span<const std::uint8_t> context = {});
  // Empty for bytes that are no membership.
  static std::vector<std::uint8_t> Context(std::span<const std::uint8_t> bytes);
  static std::optional<std::vector<RaftVoter>> Decode(
      std::span<const std::uint8_t> bytes);
};

enum class RaftMembershipChangeKind : std::uint8_t {
  AddVoter,
  RemoveVoter,
  // The same voters with another context.
  SetContext,
};

// One voter at a time, so that every majority of the old membership meets
// every majority of the new one.
struct RaftMembershipChange {
  RaftMembershipChangeKind kind = RaftMembershipChangeKind::AddVoter;
  RaftVoter voter;
  // With SetContext; with RemoveVoter, when not empty, the context the
  // same entry carries, so that what the cluster keeps of the member goes
  // with its vote.
  std::vector<std::uint8_t> context = {};
};

enum class RaftMembershipOutcome : std::uint8_t {
  Accepted,
  // Not the leader, or handing leadership over.
  NotLeader,
  // A membership entry is not committed yet, or no entry of this term is:
  // the next change waits for both.
  Busy,
  // Not a member of the cluster, its node already votes with other storage,
  // not in the membership, or the last voter.
  Invalid,
  // The leader does not see the voter's storage, or it holds less than
  // what is committed.
  NotCaughtUp,
  // Asked of the cluster rather than of the core: without the voter, the
  // ones that answer would not be a majority of those left.
  NotCarried,
};

}  // namespace stepraft
