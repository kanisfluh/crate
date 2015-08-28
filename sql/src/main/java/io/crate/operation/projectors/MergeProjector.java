/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.projectors;

import com.google.common.collect.Ordering;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MergeProjector implements Projector  {

    private final Ordering<Row> ordering;
    private final List<MergeProjectorDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean downstreamAborted = new AtomicBoolean(false);
    private final LowestCommon lowestCommon = new LowestCommon();
    private RowDownstreamHandle downstreamContext;

    public MergeProjector(int[] orderBy,
                          boolean[] reverseFlags,
                          Boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.rowOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    private static ESLogger LOGGER = Loggers.getLogger(MergeProjector.class);

    @Override
    public void startProjection(ExecutionState executionState) {
        if (remainingUpstreams.get() == 0) {
            upstreamFinished();
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        upstreams.add(upstream);
        remainingUpstreams.incrementAndGet();
        MergeProjectorDownstreamHandle handle = new MergeProjectorDownstreamHandle(this, upstream);
        LOGGER.trace("registerUpstream {} : {}", this, handle.ident);
        downstreamHandles.add(handle);
        lowestCommon.unexhaustedHandles.incrementAndGet();
        return handle;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        downstreamContext = downstream.registerUpstream(this);
    }

    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            //LOGGER.trace("comparisons: {}", comparisons.get());
            if (downstreamContext != null) {
                downstreamContext.finish();
            }
        }
    }

    protected void upstreamFailed(Throwable throwable) {
        downstreamAborted.compareAndSet(false, true);
        if (remainingUpstreams.decrementAndGet() == 0) {
            if (downstreamContext != null) {
                downstreamContext.fail(throwable);
            }
        }
    }

    @Override
    public void pause() {
        for (MergeProjectorDownstreamHandle handle : downstreamHandles) {
            if (!handle.isFinished()) {
                handle.upstream.pause();
            }
        }
    }

    @Override
    public void resume(boolean async) {
        for (MergeProjectorDownstreamHandle handle : downstreamHandles) {
            if (!handle.isFinished() && handle.row != null) {
                handle.upstream.resume(async);
            }
        }
    }

    public class MergeProjectorDownstreamHandle implements RowDownstreamHandle {

        private final String ident = "" + RandomUtils.nextInt(0, 100);
        private final MergeProjector projector;
        private final RowUpstream upstream;
        private AtomicBoolean finished = new AtomicBoolean(false);
        private AtomicBoolean performedFinish = new AtomicBoolean(false);
        private Row row = null;

        public MergeProjectorDownstreamHandle(MergeProjector projector, RowUpstream upstream) {
            this.upstream = upstream;
            this.projector = projector;
        }

        @Override
        public boolean setNextRow(Row row) {
            row = new RowN(row.materialize());
            LOGGER.trace("{} setNextRow: {}", ident, row.get(0));
            if (projector.downstreamAborted.get()) {
                return false;
            }
            return lowestCommon.emitOrPause(row, this);
        }

        public boolean isFinished() {
            return finished.get();
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable);
        }

        private void pause() {
            upstream.pause();
            LOGGER.trace("{} pause", ident);
        }


        @Override
        public void finish() {
            if (finished.compareAndSet(false, true)) {
                LOGGER.trace("{} finish", ident);
                // it's not necessary to check pendingPause, because finish() and pause() will never be called in parallel
                if (row == null && performedFinish.compareAndSet(false, true)) {
                    LOGGER.trace("{} !paused - upstreamFinished() - unexhausted: {}", ident, lowestCommon.unexhaustedHandles.get());
                    lowestCommon.emitOrPause(null, this);
                    projector.upstreamFinished();
                }
            }
        }

        private void resume() {
            LOGGER.trace("{} resume", ident);
            upstream.resume(true);
        }


    }

    private class LowestCommon {

        private final AtomicInteger unexhaustedHandles = new AtomicInteger(0);
        private Row lowestToEmit = null;

        private synchronized Set<MergeProjectorDownstreamHandle> raiseLowest(Row row, MergeProjectorDownstreamHandle handle) {

            if (nextLowest.size() > 0) {
                Map.Entry<Row, Set<MergeProjectorDownstreamHandle>> next = nextLowest.remove(0);
                LOGGER.trace("Upgrading lowestToEmit lowest from {} to {}", lowestToEmit == null ? "NULL" : lowestToEmit.get(0), next.getKey() == null ? "NULL" : next.getKey().get(0));
                lowestToEmit = next.getKey();
                unexhaustedHandles.set(next.getValue().size());
                return next.getValue();
            } else {
                LOGGER.trace("Upgrading lowestToEmit lowest from {} to {}", lowestToEmit == null ? "NULL" : lowestToEmit.get(0), "NULL");
                unexhaustedHandles.set(0);
                return null;

            }
        }


        private void resumeOthers(@Nullable Set<MergeProjectorDownstreamHandle> toResume, MergeProjectorDownstreamHandle handle) {
            if (toResume == null) {
                return;
            }
            int finishedHandles = 0;
            for (MergeProjectorDownstreamHandle h : toResume) {
                if (h != handle) {
                    emitRow(h.row, h); // TODO: this should happen in another thread
                    if (h.isFinished()) {
                        // check if finish was called meanwhile
                        if (h.performedFinish.compareAndSet(false, true)) { // TODO: maybe it's possible to remove this check, which would be awesooome
                            finishedHandles += 1;
                            upstreamFinished();
                            LOGGER.trace("{} upstreamFinished: {}",handle.ident, h.ident);
                        } else {
                            LOGGER.trace("{} pendingFinished on {} already true ", handle.ident, h.ident);
                        }
                    } else {
                        h.resume();
                    }
                }
            }
            unexhaustedHandles.getAndAdd(-finishedHandles);
        }

        private boolean emitRow(Row row, MergeProjectorDownstreamHandle handle) {
            LOGGER.trace("{} emit: {}", handle.ident, row.get(0));
            handle.row = null;
            return downstreamContext.setNextRow(row);

        }

        private boolean raiseAndEmitOrPause(@Nullable Row row, MergeProjectorDownstreamHandle handle) {
            Set<MergeProjectorDownstreamHandle> toResume = raiseLowest(row, handle);
            if (toResume == null || toResume.size() == 0) {
                LOGGER.trace("{} nothing to resume, we are finsihed", handle.ident);
                return true;
            }
            resumeOthers(toResume, handle);
            if (toResume != null && toResume.contains(handle)) {
                LOGGER.trace("{} emit after raise to: {}", handle.ident, lowestToEmit.get(0));
                return emitRow(row, handle);
            } else if(unexhaustedHandles.get() == 0) {
                // every handle is exhausted after emitting, this may happen if there where paused and finished handles
                // which has been emitted now.
                LOGGER.trace("{} inner raise and emit or pause", handle.ident);
                return raiseAndEmitOrPause(row, handle);
            } else {
                handle.pause();
                return true;
            }
        }

        // returns continue
        public boolean emitOrPause(@Nullable Row row, MergeProjectorDownstreamHandle handle) {
            LOGGER.trace("{} emitOrPause start", handle.ident);
            if (row != null && isEmittable(row, handle)) {
                LOGGER.trace("{} emit directly", handle.ident);
                LOGGER.trace("{} emitOrPause end", handle.ident);
                return emitRow(row, handle);
            }
            if (row != null) {
                handle.row = row;
            }
            if (unexhaustedHandles.decrementAndGet() == 0) {
                LOGGER.trace("{} raise end emit or pause", handle.ident);
                LOGGER.trace("{} emitOrPause end", handle.ident);
                return raiseAndEmitOrPause(row, handle);
            }
            if (row != null) {
                LOGGER.trace("{} send toPause directly", handle.ident);
                handle.pause();
            }
            LOGGER.trace("{} emitOrPause end", handle.ident);
            return true;

        }

        private final List<Map.Entry<Row, Set<MergeProjectorDownstreamHandle>>> nextLowest = new ArrayList();

        public synchronized boolean isEmittable(Row row, final MergeProjectorDownstreamHandle handle) {
            if (lowestToEmit != null && ordering.compare(row, lowestToEmit) >= 0) {
                return true;
            }

            int insertIndex = 0;
            for (Map.Entry<Row, Set<MergeProjectorDownstreamHandle>> entry : nextLowest) {
                int com = ordering.compare(row, entry.getKey());
                if ( com == 0) {
                    entry.getValue().add(handle);
                    return false;
                } else if ( com > 0 ) {
                    break;
                }
                insertIndex += 1;
            }
            Map.Entry<Row, Set<MergeProjectorDownstreamHandle>> entry = new HashMap.SimpleEntry<Row, Set<MergeProjectorDownstreamHandle>>(row, new HashSet<MergeProjectorDownstreamHandle>(){{add(handle);}});
            nextLowest.add(insertIndex, entry);

            LOGGER.trace("{} nextLowest: {}", handle.ident, row.get(0));
            return false;
        }
    }
}
