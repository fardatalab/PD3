This figure compares the varying latency of local memory sizes

To recreate the main result of this figure, it suffices to show the performance at 128M of local memory for Garnet and verify that throughput is the same as with local memory.

## Steps

### PD3

On the memory node:
- Run the remote memory server: `~/memory_backend/hashmap/pd3_backend.sh` 

On the compute node:
- Run the garnet application: `~/garnet/pd3/run.sh`
- Run the DPU transfer: `~/scripts/dpu_transfer.sh`, and enter the DPU password when prompted

On the DPU:
- Run the PD3 DPU process: `cd ae/fig22 && sudo ./prefetcher_ne -- --json config.json`

On the client node:
- Run the client process: `~/garnet/fig23.sh `. This will print out the throughput of the Garnet application using only 128M of local memory, with the rest on remote memory
