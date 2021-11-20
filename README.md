# NetSIM

🚧 Work in progress! 🚧

![Python-test](https://github.com/networmix/NetSim/workflows/Python-test/badge.svg?branch=main) 

## Introduction
NetSim is planned to become a toolkit that could help network engineers (my friends and myself included) to better reason about the network designs.

Types of problems we want this toolkit to help solving (eventually):
* Flow analysis of a network graph with simulated failures
* Simulation of routing protocol behavior in stochastic environment

From a high level standpoint the system consists of few components:
* Workflow instructions
* Simple graph library
* Tool to build network graphs from their definitions
* Discrete event simulator

## How to use
```
$ docker build -t netsim .
$ docker run -it --rm --hostname netsim --name netsim-running netsim

root@netsim:/usr/src/NetSim# python3 ./netsim/cli.py examples/wf_ee509_1_1.yaml
```
