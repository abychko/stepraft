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
#include <ostream>
#include <string>
#include <vector>

#include "stepraft/cRaftNode.hpp"
#include "stepraft/sRaftMessage.hpp"
#include "stepraft/sRaftReady.hpp"

namespace stepraft {

// Writes a simulator run as NDJSON for trace validation against
// tla/Tracestepraft.tla: one line per call into a core, with the
// node's state after it and the messages it sent, and one per network
// drop or duplicate.
class RaftTraceWriter {
 public:
  explicit RaftTraceWriter(std::ostream &out) : out_(out) {}

  // The first line: the cluster's members.
  void Begin(const std::vector<std::uint32_t> &members);

  void Tick(std::uint32_t node, const RaftNode &core,
            const std::vector<RaftMessage> &sent);
  void Step(std::uint32_t node, const RaftMessage &message,
            const RaftNode &core, const std::vector<RaftMessage> &sent);
  void Propose(std::uint32_t node, bool accepted, const RaftNode &core,
               const std::vector<RaftMessage> &sent);
  void Take(std::uint32_t node, const RaftReady &ready, const RaftNode &core,
            const std::vector<RaftMessage> &sent);
  void Advance(std::uint32_t node, const RaftNode &core,
               const std::vector<RaftMessage> &sent);
  // Any other call into a core: `extra` holds its arguments as JSON
  // members, each led by a comma.
  void Call(const char *event, std::uint32_t node, const RaftNode &core,
            const std::vector<RaftMessage> &sent, const std::string &extra);
  static std::string State(const RaftNode &core);
  void Crash(std::uint32_t node);
  void Restart(std::uint32_t node, const RaftNode &core);
  void Drop(const RaftMessage &message);
  void Duplicate(const RaftMessage &message);

  std::uint64_t Lines() const { return lines_; }

 private:
  void Line(const char *event, std::uint32_t node, const RaftNode *core,
            const std::vector<RaftMessage> *sent, const std::string &extra);
  static std::string Message(const RaftMessage &message);
  static std::string Entries(const std::vector<RaftLogEntry> &entries);

  std::ostream &out_;
  std::uint64_t lines_ = 0;
};

}  // namespace stepraft
