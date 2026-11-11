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
#include <deque>
#include <optional>
#include <vector>
#include "stepraft/cRaftLogView.hpp"
#include "stepraft/sRaftHardState.hpp"
#include "stepraft/sRaftLogEntry.hpp"

namespace stepraft {

// Reference log for tests and the simulator. Append and TruncateFrom
// change the visible log at once, and each is queued as one write. Writes
// reach the disk only when the executor says so, in the order they were
// issued, as a log fsync orders them. A simulated crash replaces the
// visible log with the durable one: unsynced appends vanish and an
// unsynced truncation is undone.
class InMemoryRaftLog : public RaftLogView {
 public:
  // RaftLogView.
  std::uint64_t FirstIndex() const override;
  std::uint64_t LastIndex() const override;
  std::optional<std::uint64_t> TermAt(std::uint64_t index) const override;
  std::vector<RaftLogEntry> Read(std::uint64_t first, std::uint64_t last,
                                 std::uint64_t maxBytes) const override;
  std::vector<RaftLogEntry> MembershipEntries(
      std::uint64_t after) const override;

  // Executor side. Append requires contiguous indexes (entry.index ==
  // LastIndex()+1, ...); returns false and appends nothing otherwise.
  bool Append(const std::vector<RaftLogEntry> &entries);
  // Explicit truncation: drop entries from `index` on (inclusive).
  // Returns false when index is below FirstIndex() (committed/evicted
  // prefix is never truncated).
  bool TruncateFrom(std::uint64_t index);
  // Evict the prefix up to `index` inclusive (retention), from the
  // visible and the durable log at once.
  bool EvictPrefixTo(std::uint64_t index);
  // Drops every entry and starts the log after (index, term), which it
  // keeps as its base; durable at once.
  void ResetTo(std::uint64_t index, std::uint64_t term);

  // Durability model. A write is one successful Append or TruncateFrom.
  std::uint64_t WriteSequence() const { return issued_; }
  // Every write up to and including `sequence` is on disk.
  void MarkDurableThrough(std::uint64_t sequence);
  std::uint64_t DurableLastIndex() const;
  // Term of a durable entry; none past the durable end.
  std::optional<std::uint64_t> DurableTermAt(std::uint64_t index) const;
  // Simulated crash.
  void TrimToDurable();

  // Durable node state lives next to the log in this reference
  // implementation; real implementations may keep it elsewhere.
  void SetHardState(const RaftHardState &state) { hardState_ = state; }
  const RaftHardState &HardState() const { return hardState_; }

 private:
  struct Write {
    std::optional<std::uint64_t> truncateFrom;
    std::vector<RaftLogEntry> entries;
  };
  void Apply(std::deque<RaftLogEntry> &log, const Write &write) const;

  std::deque<RaftLogEntry> entries_;
  std::deque<RaftLogEntry> durable_;
  std::uint64_t firstIndex_ = 1;
  // Term of the entry before firstIndex_, once one was evicted or reset.
  std::uint64_t baseTerm_ = 0;
  std::deque<Write> unsynced_;
  std::uint64_t issued_ = 0;
  std::uint64_t synced_ = 0;
  RaftHardState hardState_;
};

}  // namespace stepraft
