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

1. Clone this repo (or download as ZIP archive)
1. Build docker image:

    ```text
    docker build -t netsim .
    ```

1. Run a container:

    ```text
    docker run -it --rm --hostname netsim --name netsim-running -p 8888:8888 netsim
    ```

1. Upon startup your container started a Jupyter Notebook Server that printed a bunch of text. There should be a URL looking similar to:

    ```text
    http://127.0.0.1:8888/?token=553ef7c73f079ac51936b542f64cf58b7ea8da6561499350
    ```

1. Copy/paste the URL into your browser. It should open Jupyter Notebook App window.
1. Pick any example and run it.
