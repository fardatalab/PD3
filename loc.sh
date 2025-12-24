
TOTALC_NE=`( find ./src/PD3/network_engine -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_NE=`( find ./src/PD3/network_engine -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Network Engine LoC = $(( $TOTALC_NE + $TOTALH_NE ))"

TOTALC_TE=`( find ./src/PD3/transfer_engine -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_TE=`( find ./src/PD3/transfer_engine -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Transfer Engine LoC = $(( $TOTALC_TE + $TOTALH_TE ))"

TOTALC_PRE=`( find ./src/PD3/prefetcher -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_PRE=`( find ./src/PD3/prefetcher -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Prefetcher LoC = $(( $TOTALC_PRE + $TOTALH_PRE ))"

TOTALC_BU=`( find ./src/PD3/buffer -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_BU=`( find ./src/PD3/buffer -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Buffer LoC = $(( $TOTALC_BU + $TOTALH_BU ))"

TOTALC_LZ=`( find ./src/PD3/loading_zone -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_LZ=`( find ./src/PD3/loading_zone -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Loading Zone LoC = $(( $TOTALC_LZ + $TOTALH_LZ ))"

TOTALC_RB=`( find ./src/PD3/rdma_backend -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_RB=`( find ./src/PD3/rdma_backend -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "RDMA Backend LoC = $(( $TOTALC_RB + $TOTALH_RB ))"

TOTALC_LIB_COMMON=`( find ./src/libs/common -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TOTALH_LIB_COMMON=`( find ./src/libs/common -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Common Library LoC = $(( $TOTALC_LIB_COMMON + $TOTALH_LIB_COMMON ))"

TOTALC_EXEC=`( find ./src/apps/prefetcher_ne -name '*.c' -print0 | xargs -0 cat ) | wc -l`
TOTALH_EXEC=`( find ./src/apps/prefetcher_ne -name '*.hpp' -print0 | xargs -0 cat ) | wc -l`
echo "Prefetcher NE LoC = $(( $TOTALC_EXEC + $TOTALH_EXEC ))"

echo "Total LoC = $(( $TOTALC_NE + $TOTALH_NE + $TOTALC_TE + $TOTALH_TE + $TOTALC_PRE + $TOTALH_PRE + $TOTALC_BU + $TOTALH_BU + $TOTALC_LZ + $TOTALH_LZ + $TOTALC_RB + $TOTALH_RB + $TOTALC_LIB_COMMON + $TOTALH_LIB_COMMON + $TOTALC_EXEC + $TOTALH_EXEC ))"