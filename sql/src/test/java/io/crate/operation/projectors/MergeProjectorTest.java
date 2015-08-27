/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MergeProjectorTest extends CrateUnitTest {

    private static Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        return new RowN(cells);
    }

    private static class Upstream implements RowUpstream {

        private final ArrayList<Object[]> rows;
        private final MergeProjector.MergeProjectorDownstreamHandle downstreamHandle;
        private boolean paused = false;
        private boolean finished = false;
        private boolean threaded = true;

        public Upstream(MergeProjector projector, Object[]... rows) {
            this.rows = new ArrayList<>();
            for (int i = 0; i < rows.length; i++) {
                this.rows.add(rows[i]);
            }
            downstreamHandle = (MergeProjector.MergeProjectorDownstreamHandle)projector.registerUpstream(this);
        }

        @Override
        public void pause() {
            if (!paused) {
                paused = true;
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                int i = 0;
            }
        }

        @Override
        public void resume(boolean threaded) {
            if (paused) {
                paused = false;
                start();
            } else {
                rows.size();
            }
        }

        private void doStart() {
            while (rows.size() > 0) {
                if (paused) {
                    return;
                }
                Object[] row = rows.remove(0);
                downstreamHandle.setNextRow(spare(row));
                if (rows.size() == 0 && threaded) {
                    finish();
                }
            }
            if (finished && rows.size() == 0) {

                downstreamHandle.finish();
                return;
            }
        }

        public void start() {
            if (threaded) {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        doStart();
                    }
                });
                thread.start();
            } else {
                doStart();
            }
        }

        public void setNextRow(Object[] row) {
            rows.add(row);
            start();
        }

        public void finish() {
            finished = true;
            start();
        }
    }

    @Test
    @Repeat(iterations = 500)
    @TestLogging("io.crate.operation.projectors:TRACE")
    public void testSortMergeThreaded() throws Exception {
        MergeProjector projector = new MergeProjector(
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );

        Upstream upstream1 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{4});
        Upstream upstream2 = new Upstream(projector, new Object[]{2}, new Object[]{3}, new Object[]{5});
        Upstream upstream3 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{3}, new Object[]{4});
        Upstream upstream4 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{4});

        CollectingProjector collectingProjector = new CollectingProjector();

        projector.downstream(collectingProjector);
        projector.startProjection(mock(ExecutionState.class));

        upstream1.start();
        upstream2.start();
        upstream3.start();
        upstream4.start();

        try {
            Bucket result = collectingProjector.result().get(10, TimeUnit.SECONDS);

        assertThat(result.size(), is(13));
        assertThat((Integer) collectingProjector.rows.get(0)[0], is(1));
        assertThat((Integer) collectingProjector.rows.get(1)[0], is(1));
        assertThat((Integer) collectingProjector.rows.get(2)[0], is(1));
        assertThat((Integer) collectingProjector.rows.get(3)[0], is(2));
        assertThat((Integer) collectingProjector.rows.get(4)[0], is(3));
        assertThat((Integer) collectingProjector.rows.get(5)[0], is(3));
        assertThat((Integer) collectingProjector.rows.get(6)[0], is(3));
        assertThat((Integer) collectingProjector.rows.get(7)[0], is(3));
        assertThat((Integer) collectingProjector.rows.get(8)[0], is(3));
        assertThat((Integer) collectingProjector.rows.get(9)[0], is(4));
        assertThat((Integer) collectingProjector.rows.get(10)[0], is(4));
        assertThat((Integer) collectingProjector.rows.get(11)[0], is(4));
        assertThat((Integer) collectingProjector.rows.get(12)[0], is(5));

        } catch (TimeoutException e) {
            int i=0;
            e.printStackTrace();
        }

    }

//    @Test
//    public void testSortMerge() throws Exception {
//        MergeProjector projector = new MergeProjector(
//                new int[]{0},
//                new boolean[]{false},
//                new Boolean[]{null}
//        );
//
//        Upstream upstream1 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{4});
//        Upstream upstream2 = new Upstream(projector, new Object[]{2}, new Object[]{3});
//        Upstream upstream3 = new Upstream(projector, new Object[]{1}, new Object[]{3});
//        upstream1.threaded = false;
//        upstream2.threaded = false;
//        upstream3.threaded = false;
//
//        CollectingProjector collectingProjector = new CollectingProjector();
//
//        projector.downstream(collectingProjector);
//        projector.startProjection(mock(ExecutionState.class));
//
//        upstream1.start();
//
//        /**
//         *      Handle 1        Handle 2        Handle 3
//         *      1
//         *      PAUSE
//         */
//        assertThat(upstream1.paused, is(true));
//        assertThat(collectingProjector.rows.size(), is(0));
//
//        upstream2.start();
//        /**
//         *      Handle 1        Handle 2        Handle 3
//         *      1               2
//         *      PAUSE           PAUSE
//         */
//        assertThat(upstream2.paused, is(true));
//
//        upstream3.start();
//        /**
//         *      Handle 1        Handle 2        Handle 3
//         *      1               2               1
//         *      3               3               3
//         *      PAUSE                           PAUSE
//         *      4
//         *      {1,2,3} are emitted now
//         */
//
//        assertThat(collectingProjector.rows.size(), is(6)); // TODO: maybe manage that it's six
//        assertThat((Integer)collectingProjector.rows.get(0)[0], is(1));
//        assertThat((Integer)collectingProjector.rows.get(1)[0], is(1));
//        assertThat((Integer)collectingProjector.rows.get(2)[0], is(2));
//        assertThat((Integer)collectingProjector.rows.get(3)[0], is(3));
//        assertThat((Integer)collectingProjector.rows.get(4)[0], is(3));
//        assertThat((Integer)collectingProjector.rows.get(5)[0], is(3));
//
//        assertThat(upstream1.paused, is(true));
//        assertThat(upstream2.paused, is(false));
//        assertThat(upstream3.paused, is(true));
//
//        upstream2.setNextRow(new Object[]{3});
//        /**
//         *      Handle 1        Handle 2        Handle 3
//         *      4               3              3
//         *      PAUSED                          PAUSED
//         *      3 is emitted immediately
//         */
//        assertThat(collectingProjector.rows.size(), is(7));
//        assertThat((Integer)collectingProjector.rows.get(6)[0], is(3));
//
//        upstream3.setNextRow(new Object[]{4});
//        upstream3.finish(); // finish upstream, with non empty handle
//        /**
//         *      Handle 1        Handle 2        Handle 3 (finished)
//         *      4                               4
//         *
//         */
//
//        upstream2.setNextRow(new Object[]{5});
//        /**
//         *      Handle 1        Handle 2        Handle 3 (finished)
//         *      4               5               4
//         *
//         *      4 is emitted
//         */
//        //assertThat(collectingProjector.rows.size(), is(9));
//

//        upstream1.finish();
//        upstream2.finish();
//        /**
//         *      Handle 1 (finished)        Handle 2        Handle 3 (finished)
//         *                                 5
//         *
//         *      5 is emitted
//         */
//        assertThat(collectingProjector.rows.size(), is(10));
//        assertThat((Integer)collectingProjector.rows.get(7)[0], is(4));
//        assertThat((Integer)collectingProjector.rows.get(8)[0], is(4));
//        assertThat((Integer)collectingProjector.rows.get(9)[0], is(5));
//    }

}
