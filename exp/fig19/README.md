This figure compares throughput for PD3, PD3 (without prefetching) and local memory (entire working set in memory)

## Steps

### PD3 

On the memory node: 
- Run the remote memory server: `~/memory_backend/hashmap/pd3_backend.sh`

On the compute node:
- Run the hashmap application: `~/hashmap/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig19 && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/hashmap/fig19.sh 8 PD3 ~/fig19`. This will generate result files under the supplied directory.


### PD3 without Prefetching

On the memory node: 
- Run the remote memory server: `~/memory_backend/hashmap/pd3_backend.sh`

On the compute node:
- Run the hashmap application: `~/hashmap/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig19 && sudo ./prefetcher_ne -- --json config_np.json`

On the client node:
- Run the client process: `~/hashmap/fig19.sh 8 PD3 ~/fig19`. This will generate result files under the supplied directory.

### Local Memory

On the compute node:
- Run the hasmap application: `~/hashmap/local/run.sh`

On the client node:
- Run the client process: `~/hashmap/fig19.sh 8 PD3 ~/fig19`. This will generate result files under the supplied directory