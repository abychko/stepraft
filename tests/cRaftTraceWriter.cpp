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

#include "cRaftTraceWriter.hpp"

namespace stepraft {
namespace {

std::string Number(std::uint64_t value) { return std::to_string(value); }

std::string Flag(bool value) { return value ? "true" : "false"; }

const char *RoleName(RaftRole role) {
  switch (role) {
    case RaftRole::Follower:
      return "F";
    case RaftRole::Candidate:
      return "C";
    case RaftRole::Leader:
      return "L";
  }
  return "?";
}

}  // namespace

std::string RaftTraceWriter::Entries(const std::vector<RaftLogEntry> &entries) {
  std::string out = "[";
  for (std::size_t i = 0; i < entries.size(); ++i) {
    if (i) out += ",";
    out += "[" + Number(entries[i].index) + "," + Number(entries[i].term) +
           "," + Number(static_cast<std::uint64_t>(entries[i].kind)) + "]";
  }
  return out + "]";
}

std::string RaftTraceWriter::Message(const RaftMessage &m) {
  return "{\"type\":" + Number(static_cast<std::uint64_t>(m.type)) +
         ",\"from\":" + Number(m.from) + ",\"to\":" + Number(m.to) +
         ",\"term\":" + Number(m.term) + ",\"pi\":" + Number(m.prevLogIndex) +
         ",\"pt\":" + Number(m.prevLogTerm) + ",\"ent\":" + Entries(m.entries) +
         ",\"c\":" + Number(m.leaderCommit) + ",\"tr\":" + Flag(m.transfer) +
         ",\"hb\":" + Flag(m.heartbeat) + ",\"g\":" + Flag(m.granted) +
         ",\"ok\":" + Flag(m.success) + ",\"ci\":" + Number(m.conflictIndex) +
         ",\"ct\":" + Number(m.conflictTerm) +
         ",\"mi\":" + Number(m.matchIndex) + "}";
}

std::string RaftTraceWriter::State(const RaftNode &core) {
  std::string log = "[";
  for (std::uint64_t i = core.FirstIndex(); i <= core.LastIndex(); ++i) {
    if (i != core.FirstIndex()) log += ",";
    log += Number(core.TermAt(i).value_or(0));
  }
  log += "]";
  return "{\"role\":\"" + std::string(RoleName(core.Role())) +
         "\",\"term\":" + Number(core.Term()) +
         ",\"commit\":" + Number(core.CommitIndex()) +
         ",\"leader\":" + Number(core.LeaderId()) +
         ",\"first\":" + Number(core.FirstIndex()) + ",\"log\":" + log + "}";
}

void RaftTraceWriter::Line(const char *event, std::uint32_t node,
                           const RaftNode *core,
                           const std::vector<RaftMessage> *sent,
                           const std::string &extra) {
  out_ << "{\"e\":\"" << event << "\",\"n\":" << node;
  if (core) out_ << ",\"s\":" << State(*core);
  if (sent) {
    out_ << ",\"out\":[";
    for (std::size_t i = 0; i < sent->size(); ++i)
      out_ << (i ? "," : "") << Message((*sent)[i]);
    out_ << "]";
  }
  out_ << extra << "}\n";
  ++lines_;
}

void RaftTraceWriter::Begin(const std::vector<std::uint32_t> &members) {
  out_ << "{\"e\":\"init\",\"nodes\":[";
  for (std::size_t i = 0; i < members.size(); ++i)
    out_ << (i ? "," : "") << members[i];
  out_ << "]}\n";
  ++lines_;
}

void RaftTraceWriter::Tick(std::uint32_t node, const RaftNode &core,
                           const std::vector<RaftMessage> &sent) {
  Line("tick", node, &core, &sent, "");
}

void RaftTraceWriter::Step(std::uint32_t node, const RaftMessage &message,
                           const RaftNode &core,
                           const std::vector<RaftMessage> &sent) {
  Line("step", node, &core, &sent, ",\"m\":" + Message(message));
}

void RaftTraceWriter::Propose(std::uint32_t node, bool accepted,
                              const RaftNode &core,
                              const std::vector<RaftMessage> &sent) {
  Line("propose", node, &core, &sent, ",\"ok\":" + Flag(accepted));
}

void RaftTraceWriter::Take(std::uint32_t node, const RaftReady &ready,
                           const RaftNode &core,
                           const std::vector<RaftMessage> &sent) {
  std::string hs = "[]";
  if (ready.hardState)
    hs = "[" + Number(ready.hardState->term) + "," +
         Number(ready.hardState->votedFor.value_or(0)) + "," +
         Number(ready.hardState->commitIndex) + "]";
  std::string reset = "[]";
  if (ready.resetTo)
    reset = "[" + Number(ready.resetTo->index) + "," +
            Number(ready.resetTo->term) + "]";
  Line("take", node, &core, &sent,
       ",\"r\":{\"app\":" + Entries(ready.entriesToAppend) + ",\"trunc\":" +
           Number(ready.truncateFrom.value_or(0)) + ",\"reset\":" + reset +
           ",\"hs\":" + hs + ",\"sync\":" + Flag(ready.mustSync) + "}");
}

void RaftTraceWriter::Advance(std::uint32_t node, const RaftNode &core,
                              const std::vector<RaftMessage> &sent) {
  Line("advance", node, &core, &sent, "");
}

void RaftTraceWriter::Call(const char *event, std::uint32_t node,
                           const RaftNode &core,
                           const std::vector<RaftMessage> &sent,
                           const std::string &extra) {
  Line(event, node, &core, &sent, extra);
}

void RaftTraceWriter::Crash(std::uint32_t node) {
  Line("crash", node, nullptr, nullptr, "");
}

void RaftTraceWriter::Restart(std::uint32_t node, const RaftNode &core) {
  Line("restart", node, &core, nullptr, "");
}

void RaftTraceWriter::Drop(const RaftMessage &message) {
  Line("drop", message.to, nullptr, nullptr, ",\"m\":" + Message(message));
}

void RaftTraceWriter::Duplicate(const RaftMessage &message) {
  Line("dup", message.to, nullptr, nullptr, ",\"m\":" + Message(message));
}

}  // namespace stepraft
