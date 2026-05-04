This figure compares the latency for the YCSB Uniform workload for the hashmap application. 

## Steps

### Sync RDMA

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 51216 -q` 

On the compute node:
- Run the hashmap application: `~/hashmap/sync_rdma/hashmap_server`
- Wait until the following log line appears: `Server listening on 0.0.0.0:12345`

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig15`
- Run the client process: `./run.sh SYNC_RDMA`. This will generate result files under the `~/exp/fig15` directory

Clean Up:
- `Ctrl-C` the hashmap server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### Redy

On the memory node:
- Run the remote memory server: `~/memory_backend/batched_memory_backend` 

On the compute node:
- Run the hashmap application: `~/hashmap/redy/hashmap_server`

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig15`
- Run the client process: `~/run.sh REDY`. This will generate result files under the `~/exp/fig15` directory

Clean Up:
- `Ctrl-C` the hashmap server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### Leap

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 51216 -q` 

On the compute node:
- Run the hashmap application: `~/hashmap/sync_rdma/hashmap_server`

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig15`
- Run the client process: `./run.sh LEAP`. This will generate result files under the `~/exp/fig15` directory

Clean Up:
- `Ctrl-C` the hashmap server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/pd3_hm_memory_backend` 

The following steps need to be run for every thread count, in the order described below. The above step to run the memory server only needs to be run once before starting the steps below.

On the compute node:
- Run the hashmap application: `~/hashmap/pd3/hashmap_server`
- Wait until the following log line appears: `Server listening on 0.0.0.0:12345`
- Run the DPU transfer: `~/dpu_transfer.sh`, and enter the DPU password when prompted (you will be prompted twice)

Since the hashmap server needs to be restarted between every run, we need to warm-up. 
On the client node:
- Run the warm-up process: `~/hashmap/warmup.sh`

On the DPU:
- Navigate to the AEC directory: `cd nsdi26ae`
- Run the PD3 DPU process: `sudo hashmap/prefetcher_ne_app -l 7 -- -a aux/3,dv_flow_en=2 --config hashmap/lat_config.json `
- Wait until the following log line appears: `Starting packet polling loop...` (15-30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig15`
- Run the client process: `./run_pd3.sh <num_threads>`. This will generate result files under the `~/exp/fig15` directory. `<num_threads>` will be 1, 2, 4 and 8.

Clean up:
- Terminate the PD3 process on the DPU via `Ctrl-C`
- Terminate the hashmap process on the host via `Ctrl-C`

Post all the PD3 runs, clean up the memory backend with `Ctrl-C`

### Generate the Figure 
```
cd ~/exp/fig15/
./gen_fig.py
```


