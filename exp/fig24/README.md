This figure compares the varying latency of loading zone sizes

## Steps

### PD3

For each of the loading zone sizes, do the following. The loading zone sizes are 1m, 4m, 16m, 64m, and 256m

On the memory node:
- Run the remote memory server: `~/memory_backend/hashmap/pd3_backend.sh` 

On the compute node:
- Run the garnet application: `~/garnet/pd3/run_lz_<size>.sh`, e.g. `~/garnet/pd3/run_lz_1m.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig25_lz_<size> && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/garnet/fig25.sh `. This will print out the throughput of the Garnet application using only 128M of local memory, with the rest on remote memory
