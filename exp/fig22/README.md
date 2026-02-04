This figure compares the varying throughput of local memory sizes

To recreate the main result of this figure, it suffices to show the performance at 128M of local memory for Garnet and verify that throughput is the close as with local memory.

## Steps

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/pd3_hm_memory_backend` 

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/pd3/publish/GarnetServer.dll --storage-tier --no-obj --memory 64m --segment 32m --use-native-device-linux false --bind 10.10.2.101 --no-pubsub --index 128m`
- Wait until the following log line appears: `* Ready to accept connections`
- Run the DPU transfer: `~/dpu_transfer.sh`, and enter the DPU password when prompted (you will be prompted twice)

On the client node:
- Load data into the Garnet application: `/home/nsdi26ae/garnet/load_data.sh` (this will take about 4-5 minutes)

On the DPU:
- Navigate to the AEC directory: `cd nsdi26ae`
- Run the PD3 DPU process: `sudo garnet_small/prefetcher_ne_app -l 7 -- -a aux/3,dv_flow_en=2 --config garnet_small/app_config.json `
- Wait until the following log line appears: `Starting packet polling loop...` (15-30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig14`
- Run the client process: `./run_pd3.sh 8`. This will print out the throughput for PD3 with 8 threads. An expected value is around ~3.8-4.5 MOps

Clean up:
- Terminate the PD3 process on the DPU via `Ctrl-C`
- Terminate the Garnet process on the host via `Ctrl-C`
- Terminate the remote memory server on the memory node via `Ctrl-C`