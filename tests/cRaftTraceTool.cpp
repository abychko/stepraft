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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>

#include "cRaftSweep.hpp"
#include "cRaftTraceWriter.hpp"
#include "sSweepCoverage.hpp"

namespace stepraft {
namespace {

struct TraceArguments {
  std::string out;
  std::string plan = "base";
  std::uint32_t nodes = 3;
  std::uint64_t firstSeed = 1;
  std::uint64_t seeds = 10;
  std::uint64_t ticks = 300;
};

bool Parse(int argc, char **argv, TraceArguments &arguments) {
  for (int i = 1; i + 1 < argc; i += 2) {
    const char *value = argv[i + 1];
    const std::uint64_t number = std::strtoull(value, nullptr, 10);
    if (std::strcmp(argv[i], "--out") == 0) {
      arguments.out = value;
    } else if (std::strcmp(argv[i], "--plan") == 0) {
      arguments.plan = value;
    } else if (std::strcmp(argv[i], "--nodes") == 0) {
      arguments.nodes = static_cast<std::uint32_t>(number);
    } else if (std::strcmp(argv[i], "--first-seed") == 0) {
      arguments.firstSeed = number;
    } else if (std::strcmp(argv[i], "--seeds") == 0) {
      arguments.seeds = number;
    } else if (std::strcmp(argv[i], "--ticks") == 0) {
      arguments.ticks = number;
    } else {
      return false;
    }
  }
  return argc % 2 == 1 && !arguments.out.empty() && arguments.nodes >= 3 &&
         arguments.seeds > 0 &&
         (arguments.plan == "base" || arguments.plan == "crash");
}

}  // namespace
}  // namespace stepraft

int main(int argc, char **argv) {
  using namespace stepraft;
  TraceArguments arguments;
  if (!Parse(argc, argv, arguments)) {
    std::fprintf(stderr,
                 "usage: %s --out DIR [--plan base|crash] [--nodes N>=3] "
                 "[--first-seed N] [--seeds N] [--ticks N]\n",
                 argv[0]);
    return 2;
  }
  // base: elections and replication on a lossy network, no crashes, no
  // transfers, no membership changes. crash: base with nodes crashing and
  // restarting.
  SweepPlan plan;
  plan.transfers = false;
  plan.crashes = arguments.plan == "crash";
  SweepCoverage coverage;
  for (std::uint64_t seed = arguments.firstSeed;
       seed < arguments.firstSeed + arguments.seeds; ++seed) {
    const std::string path = arguments.out + "/" + arguments.plan + "-" +
                             std::to_string(arguments.nodes) + "-" +
                             std::to_string(seed) + ".ndjson";
    std::ofstream file(path);
    if (!file) {
      std::fprintf(stderr, "cannot write %s\n", path.c_str());
      return 1;
    }
    RaftTraceWriter writer(file);
    plan.trace = &writer;
    if (const auto broken = RaftSweep::RunSeed(
            arguments.nodes, seed, arguments.ticks, coverage, plan)) {
      std::fprintf(stderr, "nodes=%u seed=%llu\n%s", arguments.nodes,
                   static_cast<unsigned long long>(seed), broken->c_str());
      return 1;
    }
    file.close();
    if (!file) {
      std::fprintf(stderr, "cannot write %s\n", path.c_str());
      return 1;
    }
    std::printf("%s %llu lines\n", path.c_str(),
                static_cast<unsigned long long>(writer.Lines()));
  }
  return 0;
}
