# NetSim

[![Python-test](https://github.com/networmix/NetSim/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/networmix/NetSim/actions/workflows/python-test.yml)

## Introduction

NetSim is a discrete event simulation toolkit adapted for a variety of network simulation use-cases.  
It enables modeling, simulating, and analyzing network topologies, packet flows, and system behaviors under different policies and conditions.

### What Problems Can NetSim Solve?
- **Packet Queueing in Network Devices**  
  Model FIFO, RED, tail-drop, and other queue disciplines. Investigate performance metrics such as packet loss, queue occupancy, and latency under different traffic loads.
- **Flow-Based Analysis**  
  Explore how different flow rates, flow volumes, and congestion-control strategies impact network performance in switches, routers, or other custom nodes.
- **Advanced Topology Simulations**  
  Simulate networks with multiple switches, hosts, and complex packet-processing pipelines. Attach custom modules (e.g., PacketProcessors) for specialized logic.

## Key Features

- **Discrete Event Engine**  
  Built around an event-based simulation core.  
- **Flexible Network Objects**  
  Includes packet sources, switches, and sinks that can be combined to form multi-layered topologies.  
- **Queueing Models**  
  Several queueing approaches such as FIFO, tail-drop, and RED (Random Early Detection) are supported out of the box.  
- **Statistical Tracking**  
  Provides detailed statistics for throughput (bytes/packets per second), latency, packet drops, queue length, and more.  
- **Modular and Extensible**  
  Add new admission-control policies, scheduling algorithms, or custom processing nodes.