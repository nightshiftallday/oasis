#!/bin/bash
# Creates a Vivado xsim project for the vFPGA.
#
# Usage: setup_simulation.sh [--rdma on|off] [--decoders N] [--dir NAME]
#
# The RDMA setting is not cosmetic: with -DENABLE_RDMA=OFF the shell has no
# axis_rreq_*/axis_rrsp_* interfaces, so any top that touches them (including
# hardware/unit-tests/vfpga-tops/rdma_read_test.sv) fails elaboration. The
# default is the local path -- LocalRead + ColumnChunkDecoder -- which is what
# the decoder work is being brought up against. Use "--rdma on" for an
# RDMA-enabled project (goes to hardware/build-sim-rdma by default).

set -euo pipefail

rdma=off
decoders=1
dir=

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rdma)     rdma="$2";     shift 2 ;;
        --decoders) decoders="$2"; shift 2 ;;
        --dir)      dir="$2";      shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

case "$rdma" in
    on)  enable_rdma=ON  ;;
    off) enable_rdma=OFF ;;
    *) echo "--rdma must be 'on' or 'off'" >&2; exit 2 ;;
esac

if [[ -z "$dir" ]]; then
    if [[ "$rdma" == on ]]; then dir=build-sim-rdma; else dir=build-sim; fi
fi

cd "$(dirname "$0")/../hardware"
rm -rf "$dir"
mkdir "$dir"
cd "$dir"
echo "Creating Vivado simulation project in hardware/$dir (RDMA=$enable_rdma, decoders=$decoders)..."
/usr/bin/cmake -DENABLE_RDMA="$enable_rdma" -DN_DECODERS="$decoders" -DFDEV_NAME=u55c ..
make sim

echo
echo "Done. Run the tests with PYTHONPATH pointing at this project, e.g."
echo "  PYTHONPATH=hardware/$dir:\$PYTHONPATH python -m unittest discover -s hardware/unit-tests -p '*_test.py'"
