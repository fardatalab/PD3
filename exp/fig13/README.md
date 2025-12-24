This figure compares the throughput for uniformly accessing pages for the page server application. 

## Steps

### Sync RDMA

On the memory node:
- Run the remote memory server: `~/memory_backend/sync_rdma_backend.sh` 

On the compute node:
- Run the hashmap application: `~/page_server/sync_rdma/run.sh`

On the client node:
- Run the client process: `~/page_server/fig13.sh SYNC_RDMA ~/fig13`. This will generate result files under the supplied directory

### Redy

On the memory node:
- Run the remote memory server: `~/memory_backend/redy_backend.sh` 

On the compute node:
- Run the hashmap application: `~/page_server/redy/run.sh`

On the client node:
- Run the client process: `~/page_server/fig13.sh REDY ~/fig13`. This will generate result files under the supplied directory

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/page_server/pd3_backend.sh` 

The following steps need to be run for every thread count

On the compute node:
- Run the hashmap application: `~/page_server/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig13 && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/hashmap/fig13.sh <num_threads> PD3 ~/fig13`. This will generate result files under the supplied directory. `<num_threads>` will be 1, 2, 4 and 8.

### Generate the Figure 
```
python3 gen_fig.py
```


