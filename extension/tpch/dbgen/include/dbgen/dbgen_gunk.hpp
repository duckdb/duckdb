/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */
#pragma once

struct DBGenContext;

void load_dists(long textBufferSize, DBGenContext *ctx);
void cleanup_dists(void);
