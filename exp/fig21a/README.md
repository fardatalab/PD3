This figure compares the throughput for the YCSB ZIPF workload for the hashmap application when cores otherwise used for Redy are used for the application. 

## Steps

### Sync RDMA

On the memory node:
- Run the remote memory server: `~/memory_backend/sync_rdma_backend.sh` 

On the compute node:
- Run the hashmap application: `~/hashmap/sync_rdma/run.sh`

On the client node:
- Run the client process: `~/hashmap/fig21.sh SYNC_RDMA ~/fig21`. This will generate result files under the supplied directory

### Redy

On the memory node:
- Run the remote memory server: `~/memory_backend/redy_backend.sh` 

On the compute node:
- Run the hashmap application: `~/hashmap/redy/run.sh`

On the client node:
- Run the client process: `~/hashmap/fig21.sh REDY ~/fig21`. This will generate result files under the supplied directory

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/hashmap/pd3_backend.sh` 

On the compute node:
- Run the hashmap application: `~/hashmap/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig21 && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/hashmap/fig21.sh 16 PD3 ~/fig21`



