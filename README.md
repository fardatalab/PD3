# PD3 : Prefetching Data with DPUs for Disaggregated Memory

## Description
PD3 is a memory disaggregation solution that avoids cache misses via DPU-assisted prefetching. 

## Prerequisites
PD3 requires the following dependencies:
- `cmake >=3.16`
- `intel-tbb`
- `doca`
- `ibverbs`
- `libaio`
- `DOCA 3.2 LTS`

## Hardware
- DPU : `NVIDIA BlueField-3`
- RDMA NIC

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

On the Compute and Memory backend nodes:
```
cd PD3/
cmake -S . -B ./build
```
On the DPU:
```
cd PD3/
cmake -S . -B ./build -DBUILD_DPU=ON
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

## Reproducing Figures
Please see the `docs/` and `exp/` folders, both of which contain scripts and instructions on running experiments.