# PD3 : Prefetching Data with DPUs for Disaggregated Memory

## Description
PD3 is a memory disaggregation solution that avoids cache misses via DPU-assisted prefetching. More details can be found in [our NSDI 2026 paper](https://fardatalab.org/nsdi26-pd3.pdf).

## Prerequisites
PD3 requires the following dependencies:
- `cmake >=3.16`
- `intel-tbb`
- `doca` (version 3.2 LTS)
- `ibverbs`
- `libaio`

## Hardware
Our setup involves the following hardware:
- Atleast 2 dedicated machines (1 for compute, 1 for memory and remote clients) : Dell PowerEdge R7625 servers with AMD EPYC CPUs (24 cores @ 2.90 GHz) and 128GB DDR5 memory, running Ubuntu 22.04
- DPU platform sits at the Compute Node NIC: NVIDIA BlueField-3 DPU, with a 200Gbps network interface, 16 ARMv8 A78 cores and 32GB DDR4 memory 
- Network between compute / memory nodes : 200Gbps ConnectX links

## Cloning
This repository uses submodules, so clone with
```
git clone --recursive git@github.com:fardatalab/PD3.git
```
Alternatively, if you have already cloned, you can run to update or fetch all submodules: 
```
git submodule update --init --recursive
```

## Building
This codebase uses `cmake` as the build system. Use the following commands to build:

On the Compute node:
```
cd PD3/
cmake -S . -B ./build
make
```
On the DPU:
```
cd PD3/
cmake -S . -B ./build -DBUILD_DPU=ON
make
```
On the remote memory node:
```
cd PD3/
cmake -S . -B ./build
make pd3_memory_backend
```

## Code Structure
```
src/
├── apps                        # executables
├── config
├── libs                        # code for experiments / other components
└── PD3                         # code for core PD3 components
    ├── buffer 
    ├── common           
    ├── literals
    ├── loading_zone            # PD3 loading zone
    ├── network_engine          # PD3 network engine
    ├── prefetcher              # PD3 Host View + Prefetcher
    ├── rdma_backend            # PD3 remote memory server
    ├── system         
    └── transfer_engine         # PD3 transfer engine
```
