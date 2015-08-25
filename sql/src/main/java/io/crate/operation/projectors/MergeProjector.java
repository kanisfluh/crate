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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
        private Row row = null;

        public MergeProjectorDownstreamHandle(MergeProjector projector, RowUpstream upstream) {
            this.upstream = upstream;
            this.projector = projector;
        }

        @Override
        public boolean setNextRow(Row row) {
            row = new RowN(row.materialize());
            if (projector.downstreamAborted.get()) {
                return false;
            }
            return lowestCommon.emitOrPause(row, this);
        }

        private boolean emitNextRow(Row row) {
            boolean resume = downstreamContext.setNextRow(row);
            this.row = null;
            return resume;
        }

        public boolean isFinished() {
            return finished.get();
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable);
        }

        private void pause() {
            LOGGER.error("{} pause", ident);
            upstream.pause();
        }

        @Override
        public void finish() {
            if (finished.compareAndSet(false, true)) {
                LOGGER.error("{} finish", ident);
                // it's not necessary to check pendingPause, because finish() and pause() will never be called in parallel
                //if (row != null) { // paused, don't do anything
                //    // finished was called after resume - this is not a problem we just have to emit and go on :)
                //    lowestCommon.emitOrPause(row, this);
                //}
                //projector.upstreamFinished();
                if (row == null) {
                    LOGGER.error("{} !paused", ident);
                    lowestCommon.emitOrPause(null, this);
                    projector.upstreamFinished();
                } else {
                    boolean isLast = true;
                    for (MergeProjectorDownstreamHandle h : downstreamHandles) {
                        if (!h.isFinished()) {
                            isLast = false;
                        }
                    }
                    LOGGER.error("{} isLast: {}", ident, isLast);
                }
            }
        }

        private void resume() {
            LOGGER.error("{} resume", ident);
            upstream.resume(true);
        }


    }

    private class LowestCommon {

        private final AtomicInteger unexhaustedHandles = new AtomicInteger(0);
        private Row lowestToEmit = null;


        // returns continue
        public synchronized boolean emitOrPause(@Nullable Row row, MergeProjectorDownstreamHandle handle) {
            if (row != null && isEmittable(row)) {
                LOGGER.error("{} emit directly", handle.ident);
                return downstreamContext.setNextRow(row);
            } else if (unexhaustedHandles.decrementAndGet() == 0) {
                ArrayList<MergeProjectorDownstreamHandle> toResume = new ArrayList<>();
                handle.row = row;
                int finished = 0;
                lowestToEmit = handle.row;
                if (!handle.isFinished()) {
                    toResume.add(handle);
                }
                for (MergeProjectorDownstreamHandle h : downstreamHandles) {
                    if (h.row == null) {
                        LOGGER.error("{} h.row == null, isFinished: {}", h.ident, h.isFinished());
                        assert h.isFinished() : "unfinished handle without row :O";
                        continue;
                    }
                    // if lowestToEmit is null, the handle is finished
                    if (lowestToEmit == null) {
                        lowestToEmit = h.row;
                        toResume.add(h);
                        continue;
                    }
                    if (h == handle) {
                        continue;
                    }
                    int com = ordering.compare(h.row, lowestToEmit);
                    if (com > 0) {
                        toResume.clear();
                        toResume.add(h);
                        if (h.isFinished()) {
                            finished = 1;
                        }
                    } else if (com == 0) {
                        toResume.add(h);
                        if (h.isFinished()) {
                            finished += 1;
                        }
                    }
                }
                assert toResume.size() > 0 || finished == downstreamHandles.size() : "FATAL ERROR";
                LOGGER.error("{} unexhausted handles.set: {}", handle.ident, toResume.size() - finished);
                unexhaustedHandles.set(toResume.size() - finished);
                for (MergeProjectorDownstreamHandle h : toResume) {
                    if ( h != handle) {
                        LOGGER.error("{} emit other handle after raise: {}", handle.ident, h.ident);
                        downstreamContext.setNextRow(h.row); // TODO: this should happen in another thread
                        h.row = null;
                        if (h.isFinished()) {
                            upstreamFinished();
                        } else {
                            h.resume();
                        }
                    }
                }
                if (toResume.contains(handle)) {
                    handle.row = null;
                    LOGGER.error("{} emit after raise", handle.ident);
                    return downstreamContext.setNextRow(row);
                } else {
                    handle.pause();
                    return true;
                }
            } else if(row != null) {
                LOGGER.error("{} send toPause directly", handle.ident);
                handle.row = row;
                handle.pause();
            }
            return true;

        }

        public boolean isEmittable(Row row) {
            return lowestToEmit != null && ( lowestToEmit == row || ordering.compare(row, lowestToEmit) >= 0);
        }


        /*
        public boolean raiseLowest(Row row, MergeProjectorDownstreamHandle handle) {
            if (unexhaustedHandles.decrementAndGet() == 0) {
                // every handle is exhausted, so the LowestCommonRow must be raised
                lowestToEmit = row;
                for (MergeProjectorDownstreamHandle h : downstreamHandles) {
                    // h.row == null when finished
                    if (h != handle && h.row != null && ordering.compare(h.row, lowestToEmit) > 0) {
                        lowestToEmit = h.row;
                    }
                }
                return true;
            }
            return false;
        }

        public boolean finished(MergeProjectorDownstreamHandle handle) {
            if (unexhaustedHandles.decrementAndGet() == 0) {
                Row nextLowest = null;
                for (MergeProjectorDownstreamHandle h : downstreamHandles) {
                    if (h == handle) {
                        continue;
                    }
                    if ((h.paused.get() || h.pendingPause.get()) || (h.row == null && h.isFinished())) {
                        if (nextLowest == null) {
                            nextLowest = h.row;
                            continue;
                        }
                        if (h.row != null && ordering.compare(h.row, nextLowest) > 0) {
                            nextLowest = h.row;
                        }
                    } else {
                        // there is a running handle, which is not paused and not finished, no need to continue
                        return false;
                    }
                }
                if (nextLowest == null) {
                    // This happens if this is the last handle
                    return false;
                }
                lowestToEmit = nextLowest;
                return true;
            }
            return false;
        }

        public void resumed() {
            unexhaustedHandles.incrementAndGet();
        }*/
    }
}
