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

#include "stepraft/cInMemoryRaftLog.hpp"

#include <algorithm>

namespace stepraft {

std::uint64_t InMemoryRaftLog::FirstIndex() const {
  return entries_.empty() && firstIndex_ == 1 ? 0 : firstIndex_;
}

std::uint64_t InMemoryRaftLog::LastIndex() const {
  return firstIndex_ - 1 + entries_.size();
}

std::optional<std::uint64_t> InMemoryRaftLog::TermAt(
    std::uint64_t index) const {
  if (index == 0) return 0;
  if (index + 1 == firstIndex_) return baseTerm_;
  if (entries_.empty() || index < firstIndex_ || index > LastIndex())
    return std::nullopt;
  return entries_[index - firstIndex_].term;
}

std::vector<RaftLogEntry> InMemoryRaftLog::Read(std::uint64_t first,
                                                std::uint64_t last,
                                                std::uint64_t maxBytes) const {
  std::vector<RaftLogEntry> out;
  if (entries_.empty() || first < firstIndex_ || first > last ||
      last > LastIndex())
    return out;
  std::uint64_t bytes = 0;
  for (std::uint64_t i = first; i <= last; ++i) {
    const RaftLogEntry &entry = entries_[i - firstIndex_];
    bytes += entry.payload.size();
    if (!out.empty() && bytes > maxBytes) break;
    out.push_back(entry);
  }
  return out;
}

std::vector<RaftLogEntry> InMemoryRaftLog::MembershipEntries(
    std::uint64_t after) const {
  std::vector<RaftLogEntry> out;
  for (const RaftLogEntry &entry : entries_)
    if (entry.index > after && entry.kind == RaftEntryKind::Membership)
      out.push_back(entry);
  return out;
}

void InMemoryRaftLog::Apply(std::deque<RaftLogEntry> &log,
                            const Write &write) const {
  if (write.truncateFrom && *write.truncateFrom >= firstIndex_ &&
      *write.truncateFrom - firstIndex_ < log.size())
    log.resize(*write.truncateFrom - firstIndex_);
  log.insert(log.end(), write.entries.begin(), write.entries.end());
}

bool InMemoryRaftLog::Append(const std::vector<RaftLogEntry> &entries) {
  if (entries.empty()) return true;
  std::uint64_t next = entries_.empty() ? firstIndex_ : LastIndex() + 1;
  for (const RaftLogEntry &entry : entries) {
    if (entry.index != next++) return false;
  }
  Write write;
  write.entries = entries;
  Apply(entries_, write);
  unsynced_.push_back(std::move(write));
  ++issued_;
  return true;
}

bool InMemoryRaftLog::TruncateFrom(std::uint64_t index) {
  if (index < firstIndex_) return false;
  Write write;
  write.truncateFrom = index;
  Apply(entries_, write);
  unsynced_.push_back(std::move(write));
  ++issued_;
  return true;
}

bool InMemoryRaftLog::EvictPrefixTo(std::uint64_t index) {
  if (entries_.empty() || index < firstIndex_) return false;
  const std::uint64_t cut = std::min(index, LastIndex());
  const auto count = static_cast<std::size_t>(cut - firstIndex_ + 1);
  baseTerm_ = entries_[count - 1].term;
  entries_.erase(entries_.begin(),
                 entries_.begin() + static_cast<std::ptrdiff_t>(count));
  durable_.erase(durable_.begin(),
                 durable_.begin() + static_cast<std::ptrdiff_t>(
                                        std::min(count, durable_.size())));
  firstIndex_ = cut + 1;
  return true;
}

void InMemoryRaftLog::MarkDurableThrough(std::uint64_t sequence) {
  while (!unsynced_.empty() && synced_ < sequence) {
    Apply(durable_, unsynced_.front());
    unsynced_.pop_front();
    ++synced_;
  }
}

std::uint64_t InMemoryRaftLog::DurableLastIndex() const {
  return firstIndex_ - 1 + durable_.size();
}

std::optional<std::uint64_t> InMemoryRaftLog::DurableTermAt(
    std::uint64_t index) const {
  if (index + 1 == firstIndex_) return baseTerm_;
  if (index < firstIndex_ || index > DurableLastIndex()) return std::nullopt;
  return durable_[index - firstIndex_].term;
}

void InMemoryRaftLog::ResetTo(std::uint64_t index, std::uint64_t term) {
  entries_.clear();
  durable_.clear();
  unsynced_.clear();
  synced_ = issued_;
  firstIndex_ = index + 1;
  baseTerm_ = term;
}

void InMemoryRaftLog::TrimToDurable() {
  entries_ = durable_;
  unsynced_.clear();
  synced_ = issued_;
}

}  // namespace stepraft
