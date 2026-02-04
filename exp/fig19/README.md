This figure compares throughput for PD3 and local memory (entire working set in memory).

## Steps

### Hashmap 

On the compute node:
- Run the hashmap application: `~/hashmap/local_mem/hashmap_server`
- Wait until the following log line appears: `Server listening on 0.0.0.0:12345`

Warm-up, on the client node:
- Run the warm-up process: `~/hashmap/warmup.sh`

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig19`
- Run the client process: `./run_hm.sh`. This will print out the throughput for local memory with 8 threads. A correct value is around ~60-65 MOps
- Verify that this is close to `~/exp/fig11/res.PD3.8.tput`

Clean Up:
- `Ctrl-C` the hashmap server on the compute node

### Page Server

On the compute node:
- Run the page server application: `~/pageserver/local_mem/server`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

Warm-up, on the client node:
- Run the warm-up process: `~/pageserver/warmup.sh`

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig19`
- Run the client process: `./run_ps.sh`. This will print out the throughput for local memory with 8 threads. A correct value is around ~1 MOps
- Verify that this is close to `~/exp/fig13/res.PD3.8.tput`

Clean Up:
- `Ctrl-C` the page server on the compute node

### Garnet

On the compute node:
- Run the Garnet application: `dotnet ~/garnet/sync_rdma/publish/GarnetServer.dll --storage-tier --no-obj --memory 20g --segment 32m --bind 10.10.2.101 --no-pubsub --index 256m --use-native-device-linux false`
- Wait until the following log line appears: `* Ready to accept connections` (~30s)

On the client node:
- Navigate to the experiment directory: `cd ~/exp/fig19`
- Run the client process: `./run_garnet.sh`. This will print out the throughput for PD3 with 8 threads. A correct value is around ~5 MOps
- Verify that this is close to `~/exp/fig15/res.PD3.8.tput`

Clean Up:
- `Ctrl-C` the Garnet server on the compute node
