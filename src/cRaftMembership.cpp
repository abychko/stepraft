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

#include "stepraft/cRaftMembership.hpp"

#include <algorithm>

namespace stepraft {

namespace {

void Put(std::vector<std::uint8_t> &out, std::uint64_t value, int bytes) {
  for (int i = 0; i < bytes; ++i)
    out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
}

std::uint64_t Get(std::span<const std::uint8_t> bytes, std::size_t at,
                  int length) {
  std::uint64_t value = 0;
  for (int i = 0; i < length; ++i)
    value |= static_cast<std::uint64_t>(bytes[at + static_cast<std::size_t>(i)])
             << (8 * i);
  return value;
}

}  // namespace

bool RaftMembership::Contains(const std::vector<RaftVoter> &voters,
                              std::uint32_t id, std::uint64_t storageId) {
  return std::any_of(voters.begin(), voters.end(), [&](const RaftVoter &v) {
    return v.id == id && (v.storageId == 0 || v.storageId == storageId);
  });
}

bool RaftMembership::HasNode(const std::vector<RaftVoter> &voters,
                             std::uint32_t id) {
  return std::any_of(voters.begin(), voters.end(),
                     [&](const RaftVoter &v) { return v.id == id; });
}

std::vector<std::uint8_t> RaftMembership::Encode(
    const std::vector<RaftVoter> &voters,
    std::span<const std::uint8_t> context) {
  std::vector<std::uint8_t> out;
  Put(out, voters.size(), 4);
  for (const RaftVoter &voter : voters) {
    Put(out, voter.id, 4);
    Put(out, voter.storageId, 8);
  }
  out.insert(out.end(), context.begin(), context.end());
  return out;
}

std::vector<std::uint8_t> RaftMembership::Context(
    std::span<const std::uint8_t> bytes) {
  if (bytes.size() < 4) return {};
  const std::uint64_t voters = 4 + Get(bytes, 0, 4) * 12;
  if (bytes.size() < voters) return {};
  return {bytes.begin() + static_cast<std::ptrdiff_t>(voters), bytes.end()};
}

std::optional<std::vector<RaftVoter>> RaftMembership::Decode(
    std::span<const std::uint8_t> bytes) {
  if (bytes.size() < 4) return std::nullopt;
  const std::uint64_t count = Get(bytes, 0, 4);
  if (bytes.size() < 4 + count * 12) return std::nullopt;
  std::vector<RaftVoter> voters;
  for (std::uint64_t i = 0; i < count; ++i) {
    const std::size_t at = 4 + static_cast<std::size_t>(i) * 12;
    voters.push_back(
        {static_cast<std::uint32_t>(Get(bytes, at, 4)), Get(bytes, at + 4, 8)});
  }
  return voters;
}

}  // namespace stepraft
