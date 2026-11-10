cmake_minimum_required(VERSION 3.0)
project(oasis)

set(CYT_DIR ${CMAKE_SOURCE_DIR}/../parcore/libstf/coyote)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CYT_DIR}/cmake)

find_package(CoyoteHW REQUIRED)

# Number of ColumnChunkDecoders. The number of streams is equal to this in local mode. If RDMA is 
# enabled, there will be an additional RDMA bypass stream for the RDMAFileSystem.
# Every decoder writes 2 output ports (values, string heap), the RDMA bypass 1, and the notify
# value has a 3-bit port id, so at most 8 ports fit: N_DECODERS <= 3 with RDMA, <= 4 without
# (vfpga_top.svh asserts this).
set(N_DECODERS 1 CACHE STRING "Number of ColumnChunkDecoders (default=1)")

# Include Coyote's RDMA stack during synthesis (disable with -DENABLE_RDMA=OFF)
option(ENABLE_RDMA "Enable RDMA stack" ON)

# Number of vFPGAs (user applications)
set(N_REGIONS 1)
set(EN_STRM 1)

if(ENABLE_RDMA)
    math(EXPR N_STREAMS "${N_DECODERS} + 1")
    set(EN_RDMA 1)
    set(N_RDMA_AXI ${N_STREAMS})
    set(N_STRM_AXI ${N_STREAMS})
else()
    set(EN_RDMA 0)
    set(N_STRM_AXI ${N_DECODERS})
endif()

# Build with optimization, since timing closure can be difficult
set(COMP_CORES 32)
set(BUILD_OPT 1)

# Target FPGA device (override with -DFDEV_NAME=v80 for the Alveo V80)
set(FDEV_NAME u55c CACHE STRING "Target FPGA device (u55c, v80, ...)")

# Enable 1 GiB huge pages in the TLB
set(TLBL_BITS 30)

validation_checks_hw()

# ../celeris/hardware/src is omitted: the pinned celeris predates the libstf rename of
# MAXIMUM_HOST_ALLOCATION_LEN_BIT -> MAXIMUM_BUFFER_SIZE, so its `common` package no longer
# compiles. Nothing in src/ instantiates a celeris module, so this only removes dead sources.
# Re-add once the celeris submodule is re-synced against this libstf.
load_apps (
    VFPGA_C0_0 "src ../parcore/libstf/hardware/src ../parcore/vhsnunzip/src ../parcore/hardware/src"
)

create_hw()
