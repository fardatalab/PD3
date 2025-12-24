This figure compares the latency for the Resp.Bench workload for the Garnet application. 

## Steps

### Sync RDMA

On the memory node:
- Run the remote memory server: `~/memory_backend/sync_rdma_backend.sh` 

On the compute node:
- Run the hashmap application: `~/garnet/sync_rdma/run.sh`

On the client node:
- Run the client process: `~/garnet/fig18.sh SYNC_RDMA ~/fig18`. This will generate result files under the supplied directory

### Redy

On the memory node:
- Run the remote memory server: `~/memory_backend/redy_backend.sh` 

On the compute node:
- Run the hashmap application: `~/garnet/redy/run.sh`

On the client node:
- Run the client process: `~/garnet/fig18.sh REDY ~/fig18`. This will generate result files under the supplied directory

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/garnet/pd3_backend.sh` 

The following steps need to be run for every thread count

On the compute node:
- Run the hashmap application: `~/garnet/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig14 && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/hashmap/fig18.sh <num_threads> PD3 ~/fig18`. This will generate result files under the supplied directory. `<num_threads>` will be 1, 2, 4 and 8.

### Generate the Figure 
```
python3 gen_fig.py
```


