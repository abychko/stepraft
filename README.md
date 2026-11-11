# stepraft

A Raft consensus library in C++20. It implements leader election with
pre-vote and check-quorum, leadership transfer, flow-controlled log
replication and membership changes one voter at a time.

## Use

Requirements: CMake 3.20+ and a C++20 compiler.

```cmake
ADD_SUBDIRECTORY(third_party/stepraft)
TARGET_LINK_LIBRARIES(my-target PRIVATE stepraft::stepraft)
```

- [`cRaftNode.hpp`](include/stepraft/cRaftNode.hpp): `stepraft::RaftNode`,
  the entry point;
- [`sRaftReady.hpp`](include/stepraft/sRaftReady.hpp): what a step returns
  and in which order it is applied;
- [`cRaftLogView.hpp`](include/stepraft/cRaftLogView.hpp): the log
  interface.

## License

GNU General Public License, version 2.0, with a permission to link with
separately licensed software such as OpenSSL: see [`LICENSE`](LICENSE) and
the header of any source file.
