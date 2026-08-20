#pragma once

#include "duckdb.hpp"
#include "parcore/metadata/metadata.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

// Builds the ParCore metadata (column names, row groups, per-column-chunk type/compression/offset)
// from an already-opened DuckDB ParquetReader's footer. Throws InvalidInputException for Parquet
// physical types or compression codecs that ParCore does not support.
parcore::metadata::Metadata BuildParcoreMetadata(ParquetReader &parquet_reader);

// ColumnMetaData.total_uncompressed_size for every column chunk, indexed [row_group][column].
//
// This is the upper bound on a BYTE_ARRAY chunk's string heap: the heap is the chunk's PLAIN
// payload copied verbatim (length prefixes included), which is the dictionary page for a
// dictionary-encoded chunk and the data pages for a plain-encoded one -- both bounded by the
// chunk's total uncompressed size.
//
// Kept here rather than added to parcore::metadata::ColumnChunk because that struct has a
// serialized on-disk form (ColumnChunk::from), and widening it would invalidate every existing
// .meta sidecar.
vector<vector<uint64_t>> BuildUncompressedSizes(ParquetReader &parquet_reader);

} // namespace duckdb
