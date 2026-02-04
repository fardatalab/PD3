This figure compares the performance of the hashmap with 16 threads for the YCSB Uniform workload.

## Steps

### Sync RDMA

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 51216 -q` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/sync_rdma/publish/GarnetServer.dll --storage-tier --no-obj --memory 1g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig21c`
- Run the client process: `./run.sh SYNC_RDMA`. This will generate result files under the `~/exp/fig21c` directory

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### Redy

On the memory node:
- Run the remote memory server: `~/memory_backend/batched_memory_backend` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/redy/publish/GarnetServer.dll --storage-tier --no-obj --memory 1g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig21c`
- Run the client process: `./run.sh REDY`. This will generate result files under the `~/exp/fig21c` directory

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### Leap

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 51216 -q` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/sync_rdma/publish/GarnetServer.dll --storage-tier --no-obj --memory 1g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig21c`
- Run the client process: `./run.sh LEAP`. This will generate result files under the `~/exp/fig21c` directory

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/pd3_hm_memory_backend` 

The following steps need to be run for every thread count, in the order described below. The above step to run the memory server only needs to be run once before starting the steps below.

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/pd3/publish/GarnetServer.dll --storage-tier --no-obj --memory 700m --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)
- Wait until the following log line appears: `Server listening on 0.0.0.0:12345`
- Run the DPU transfer: `~/dpu_transfer.sh`, and enter the DPU password when prompted (you will be prompted twice)

On the client node:
- Load data into the Garnet application: `/home/nsdi26ae/garnet/load_data.sh` (this will take about 4-5 minutes)

On the DPU:
- Navigate to the AEC directory: `cd nsdi26ae`
- Run the PD3 DPU process: `sudo garnet/prefetcher_ne_app -l 7 -- -a aux/3,dv_flow_en=2 --config garnet/app_config.json `
- Wait until the following log line appears: `Starting packet polling loop...` (15-30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig21c`
- Run the client process: `./run_pd3.sh <num_threads>`. This will generate result files under the `~/exp/fig21c` directory. `<num_threads>` will be 8 and 16.

Clean up:
- Terminate the PD3 process on the DPU via `Ctrl-C`
- Terminate the Garnet process on the host via `Ctrl-C`

Post all the PD3 runs, clean up the memory backend with `Ctrl-C`

### Generate the Figure
```
cd ~/exp/fig21c/
./gen_fig.py
```


