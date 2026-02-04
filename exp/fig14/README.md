This figure compares the throughput for the Resp.Bench workload for the Garnet application. 

## Steps

### Sync RDMA

Note: The Garnet experiments take a while to run (~5+ minutes per run)

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 12345 -q` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/sync_rdma/publish/GarnetServer.dll --storage-tier --no-obj --memory 1g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig14`
- Run the client process: `./run.sh SYNC_RDMA`. This will generate result files under the `~/exp/fig14` directory

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
- Navigate to the experiment directory: `cd ~/exp/fig14`
- Run the client process: `./run.sh REDY`. This will generate result files under the `~/exp/fig14` directory

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
- `Ctrl-C` the remote memory server on the memory node


### Leap

On the memory node:
- Run the remote memory server: `~/memory_backend/memory_backend -a 10.10.2.100 -p 12345 -q` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/sync_rdma/publish/GarnetServer.dll --storage-tier --no-obj --memory 1g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig14`
- Run the client process: `./run.sh LEAP`. This will generate result files under the `~/exp/fig14` directory

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
- `Ctrl-C` the remote memory server on the memory node

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/pd3_hm_memory_backend` 

The following steps need to be run for every thread count, in the order described below. The above step to run the memory server only needs to be run once before starting the steps below.

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/pd3/publish/GarnetServer.dll --storage-tier --no-obj --memory 700m --segment 32m --use-native-device-linux false --bind 10.10.2.101 --no-pubsub --index 256m`
- Wait until the following log line appears: `* Ready to accept connections`
- Run the DPU transfer: `~/dpu_transfer.sh`, and enter the DPU password when prompted (you will be prompted twice)

On the client node:
- Load data into the Garnet application: `/home/nsdi26ae/garnet/load_data.sh` (this will take about 4-5 minutes)

On the DPU:
- Navigate to the AEC directory: `cd nsdi26ae`
- Run the PD3 DPU process: `sudo garnet/prefetcher_ne_app -l 7 -- -a aux/3,dv_flow_en=2 --config garnet/app_config.json `
- Wait until the following log line appears: `Starting packet polling loop...` (15-30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig14`
- Run the client process: `./run_pd3.sh <num_threads>`. This will generate result files under the `~/exp/fig14` directory. `<num_threads>` will be 1, 2, 4 and 8.

Clean up:
- Terminate the PD3 process on the DPU via `Ctrl-C`
- Terminate the Garnet process on the host via `Ctrl-C`

Post all the PD3 runs, terminate the memory backend process with `Ctrl-C`

### Generate the Figure 
```
cd ~/exp/fig14/
./gen_fig.py
```


