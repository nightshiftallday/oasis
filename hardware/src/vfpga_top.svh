`timescale 1ns / 1ps

`include "lynx_macros.svh"
`include "libstf_macros.svh"

import oasis::*;
import parcore::*;

// -- Tie-off unused interfaces and signals --------------------------------------------------------
always_comb cq_rd.tie_off_s();

`ifdef EN_RDMA
always_comb rq_rd.tie_off_s();
always_comb rq_wr.tie_off_s();

for (genvar I = 0; I < N_STRM_AXI; I++) begin
    always_comb axis_host_recv[I].tie_off_s();
end

for (genvar I = 0; I < N_RDMA_AXI; I++) begin
    always_comb axis_rrsp_send[I].tie_off_m();
    always_comb axis_rrsp_recv[I].tie_off_s();
    always_comb axis_rreq_send[I].tie_off_m();
end

`ASSERT_ELAB(N_STRM_AXI == N_RDMA_AXI)
`endif

localparam NUM_STREAMS        = N_STRM_AXI;
localparam DATABEAT_SIZE      = AXI_DATA_BITS / 8;

localparam NUM_CONFIGS   = 3;
`ifdef EN_RDMA
localparam NUM_DECODERS  = NUM_STREAMS - 1;
`else
localparam NUM_DECODERS  = NUM_STREAMS;
`endif

// The OutputWriter writes NUM_PORTS ports over the NUM_STREAMS Coyote streams. Each decoder's
// stream carries two ports, its values on port 2I and its string heap on port 2I+1; the RDMA
// bypass stream carries one, port 2 * NUM_DECODERS. The port is also the mem_config slot and the
// interrupt's stream id, so software addresses output buffers by the same number.
localparam int PORTS_PER_DECODER = 2;
`ifdef EN_RDMA
localparam int BYPASS_PORT = PORTS_PER_DECODER * NUM_DECODERS;
localparam int NUM_PORTS   = BYPASS_PORT + 1;
`else
localparam int NUM_PORTS   = PORTS_PER_DECODER * NUM_DECODERS;
`endif

function automatic logic [0:NUM_STREAMS - 1][31:0] ports_per_stream();
    for (int s = 0; s < NUM_STREAMS; s++) begin
        ports_per_stream[s] = s < NUM_DECODERS ? PORTS_PER_DECODER : 1;
    end
endfunction
localparam logic [0:NUM_STREAMS - 1][31:0] PORTS_PER_STREAM = ports_per_stream();

// The interrupt carries the port in 3 bits, so at most 8 ports: 3 decoders plus the RDMA bypass,
// or 4 decoders without RDMA. Fail here rather than deep inside the OutputWriter, where the
// number no longer looks like the decoder count.
`ASSERT_ELAB(NUM_PORTS <= 8)

// MemConfig write side needs NUM_PORTS+1 regs, read side needs 4
// (ID, num_streams, max_enqueued, empty_stream_consumes_buffer).
localparam MEM_CONFIG_NUM_REGS = (NUM_PORTS + 1 > 4) ? NUM_PORTS + 1 : 4;

// -- Fix clock and reset names --------------------------------------------------------------------
logic clk;
logic rst_n;

assign clk   = aclk;
assign rst_n = aresetn;

// -- Configuration --------------------------------------------------------------------------------
write_config_i                       write_configs[NUM_CONFIGS](.*);
read_config_i                        read_configs [NUM_CONFIGS](.*);
mem_config_i                         mem_conf[NUM_PORTS](.*);
ready_valid_i #(read_req_t)          read_conf[NUM_STREAMS](.*);
logic [PID_BITS-1:0]                 read_ctid[NUM_STREAMS];
ready_valid_i #(column_chunk_conf_t) column_chunk_conf[NUM_DECODERS](.*);
ready_valid_i #(vaddress_t)          heap_addrs[NUM_DECODERS](.*);
decoder_profile_i                    decoder_profiles[NUM_DECODERS]();

GlobalConfig #(
    .SYSTEM_ID(OASIS_SYSTEM_ID),
    .NUM_CONFIGS(NUM_CONFIGS),
    .ADDR_SPACE_SIZES({
        MEM_CONFIG_NUM_REGS,
        COLUMN_CHUNK_DECODER_READ_REGS(NUM_DECODERS),
        NUM_READ_REQ_CONFIG_REGS * NUM_STREAMS
    })
) inst_config (
    .clk(clk),
    .rst_n(rst_n),

    .axi_ctrl(axi_ctrl),

    .write_configs(write_configs),
    .read_configs(read_configs)
);

// Software enqueues one buffer per output stream of a flow ahead of time, so the empty heap stream of
// a string chunk without long strings has to consume its buffer as well.
MemConfig #(
    .NUM_STREAMS(NUM_PORTS),
    .EMPTY_STREAM_CONSUMES_BUFFER(1)
) inst_mem_config (
    .clk(clk),
    .rst_n(rst_n),

    .write_config(write_configs[0]),
    .read_config(read_configs[0]),

    .out(mem_conf)
);

// String decoders take a string chunk's heap base address through their own register
StringColumnChunkDecoderConfig #(
    .NUM_DECODERS(NUM_DECODERS)
) inst_column_chunk_decoder_config (
    .clk(clk),
    .rst_n(rst_n),

    .write_config(write_configs[1]),
    .read_config(read_configs[1]),

    .out(column_chunk_conf),
    .heap_addrs(heap_addrs),

    .profile(decoder_profiles)
);

ReadReqConfig #(
    .NUM_STREAMS(NUM_STREAMS)
) inst_read_req_config (
    .clk(clk),
    .rst_n(rst_n),

    .write_config(write_configs[2]),
    .read_config(read_configs[2]),

    .out(read_conf),
    .ctid(read_ctid)
);

// -- Arbiter the read send queue ------------------------------------------------------------------
metaIntf #(.STYPE(req_t)) sq_rd_strm [NUM_STREAMS](.aclk(clk), .aresetn(rst_n));

MetaIntfArbiter #(
    .N_INTERFACES(NUM_STREAMS),
    .STYPE(req_t)
) inst_sq_wr_arbiter (
    .clk(clk),
    .rst_n(rst_n),

    .intf_in(sq_rd_strm),
    .intf_out(sq_rd)
);

// -- Data path ------------------------------------------------------------------------------------
AXI4S axi_produced[NUM_PORTS](.aclk(clk), .aresetn(rst_n));
AXI4S axi_out     [NUM_PORTS](.aclk(clk), .aresetn(rst_n));
for (genvar I = 0; I < NUM_DECODERS; I++) begin : gen_decoders
    AXI4S axi_in (.aclk(aclk), .aresetn(aresetn));
    ndata_i #(data8_t, DATABEAT_SIZE) decoder_in(.*);
    ndata_i #(data8_t, DATABEAT_SIZE) out(.*);
    ndata_i #(data8_t, DATABEAT_SIZE) heap_out(.*);

    AXI4S axi_recv (.aclk(aclk), .aresetn(aresetn));

    // AXI4SR to AXI4S
`ifdef EN_RDMA
    `AXIS_ASSIGN(axis_rreq_recv[I], axi_recv)
`else
    `AXIS_ASSIGN(axis_host_recv[I], axi_recv)
`endif

    AXISkidBuffer inst_recv_skid_buffer (
        .clk(clk),
        .rst_n(rst_n),

        .in(axi_recv),
        .out(axi_in)
    );

`ifdef EN_RDMA
    RDMARead #(
        .AXI_STRM_ID(I),
        .DATABEAT_SIZE(DATABEAT_SIZE)
    ) inst_rdma_read (
        .clk(clk),
        .rst_n(rst_n),

        .conf(read_conf[I]),
        .ctid(read_ctid[I]),

        .sq_rd(sq_rd_strm[I]),

        .in(axi_in),
        .out(decoder_in)
    );
`else
    LocalRead #(
        .AXI_STRM_ID(I),
        .DATABEAT_SIZE(DATABEAT_SIZE)
    ) inst_local_read (
        .clk(clk),
        .rst_n(rst_n),

        .conf(read_conf[I]),
        .sq_rd(sq_rd_strm[I]),

        .in(axi_in),
        .out(decoder_in)
    );
`endif

    StringColumnChunkDecoder #(
        .DATABEAT_SIZE(DATABEAT_SIZE)
    ) inst_column_chunk_decoder (
        .clk(clk),
        .rst_n(rst_n),

        .conf(column_chunk_conf[I]),
        .heap_addr(heap_addrs[I]),

        .in(decoder_in),
        .out(out),
        .heap_out(heap_out),

        .profile(decoder_profiles[I])
    );

    // Values -> port 2I. With defer_dict_decoding, a dictionary encoded chunk's dictionary
    // followed by its 1, 2 or 4 byte indices.
    NDataToAXI #(data8_t, DATABEAT_SIZE) inst_ndata_to_axi (
        .clk(clk),
        .rst_n(rst_n),

        .in(out),
        .out(axi_produced[PORTS_PER_DECODER*I])
    );

    // String heap -> port 2I+1. Idle for every chunk that is not a german string chunk, so
    // nothing is written there and no interrupt is raised.
    NDataToAXI #(data8_t, DATABEAT_SIZE) inst_ndata_to_axi_heap (
        .clk(clk),
        .rst_n(rst_n),

        .in(heap_out),
        .out(axi_produced[PORTS_PER_DECODER*I+1])
    );
end : gen_decoders

// -- RDMA bypass stream (last stream, no decoder, one port) ---------------------------------------
`ifdef EN_RDMA
localparam BYPASS_ID = NUM_STREAMS - 1;

AXI4S axi_in (.aclk(aclk), .aresetn(aresetn));
ndata_i #(data8_t, DATABEAT_SIZE) bypass_ndata();

// AXI4SR to AXI4S
`AXIS_ASSIGN(axis_rreq_recv[BYPASS_ID], axi_in)

RDMARead #(
    .AXI_STRM_ID(BYPASS_ID),
    .DATABEAT_SIZE(DATABEAT_SIZE)
) inst_rdma_read_bypass (
    .clk(clk),
    .rst_n(rst_n),

    .conf(read_conf[BYPASS_ID]),
    .ctid(read_ctid[BYPASS_ID]),

    .sq_rd(sq_rd_strm[BYPASS_ID]),

    .in(axi_in),
    .out(bypass_ndata)
);

NDataToAXI #(data8_t, DATABEAT_SIZE) inst_ndata_to_axi_bypass (
    .clk(clk),
    .rst_n(rst_n),

    .in(bypass_ndata),
    .out(axi_produced[BYPASS_PORT])
);
`endif

// -- Output writer --------------------------------------------------------------------------------
for (genvar I = 0; I < NUM_PORTS; I++) begin : gen_out_skid_buffers
    AXISkidBuffer inst_out_skid_buffer (
        .clk(clk),
        .rst_n(rst_n),

        .in(axi_produced[I]),
        .out(axi_out[I])
    );
end

AXI4SR axi_send[NUM_STREAMS](.aclk(clk), .aresetn(rst_n));

OutputWriter #(
    .NUM_STREAMS(NUM_STREAMS),
    .PORTS_PER_STREAM(PORTS_PER_STREAM)
) inst_output_writer (
    .clk(clk),
    .rst_n(rst_n),

    .sq_wr(sq_wr),
    .cq_wr(cq_wr),
    .notify(notify),

    .mem_config(mem_conf),

    .data_in(axi_out),
    .data_out(axi_send),

    .notify_count() // Unused: oasis has no GenericConfig to expose it
);

typedef struct packed {
    logic [AXI_DATA_BITS - 1:0]     tdata;
    logic [AXI_DATA_BITS / 8 - 1:0] tkeep;
    logic [PID_BITS - 1:0]          tid;
    logic                           tlast;
} axisr_beat_t;

for (genvar I = 0; I < NUM_STREAMS; I++) begin : gen_send_skid_buffers
    ready_valid_i #(axisr_beat_t) send_in(clk, rst_n), send_out(clk, rst_n);

    assign send_in.data       = '{axi_send[I].tdata, axi_send[I].tkeep, axi_send[I].tid, axi_send[I].tlast};
    assign send_in.valid      = axi_send[I].tvalid;
    assign axi_send[I].tready = send_in.ready;

    SkidBuffer #(axisr_beat_t) inst_send_skid_buffer (
        .clk(clk),
        .rst_n(rst_n),

        .in(send_in),
        .out(send_out)
    );

    assign axis_host_send[I].tdata  = send_out.data.tdata;
    assign axis_host_send[I].tkeep  = send_out.data.tkeep;
    assign axis_host_send[I].tid    = send_out.data.tid;
    assign axis_host_send[I].tlast  = send_out.data.tlast;
    assign axis_host_send[I].tvalid = send_out.valid;
    assign send_out.ready           = axis_host_send[I].tready;
end
