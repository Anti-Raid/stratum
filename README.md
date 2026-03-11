# Stratum

Stratum is a gateway proxy between Discord and template-worker (and other Anti-Raid services) to enable for fast restarts etc. Unlike other gateway proxies, Stratum exposes all of its APIs using grpc and does not expose a standard Discord websocket system.

Stratum replaces Sandwich-Daemon (which was what we previously used) with the main benefit being that Stratum shouldn't be using 30% of our servers CPU nor have weird hard-to-debug caching issues as well.

## Libraries

- common: Contains the protobuf definition and some common protocol functions
- server: Contains the core stratum server
- client: Contains a mid/high-level client to communicate with the Stratum server and recieve Discord gateway events etc. from it.
- torracat: Contains a simple test client for testing out stratum