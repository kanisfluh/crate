/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.merge;

import com.google.common.collect.Iterators;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PassThroughPagingIteratorTest {

    @Test
    public void testHasNextCallWithoutMerge() throws Exception {
        PassThroughPagingIterator<Object> iterator = new PassThroughPagingIterator<>();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInputIsPassedThrough() throws Exception {
        PassThroughPagingIterator<String> iterator = new PassThroughPagingIterator<>();
        iterator.merge(Arrays.asList(
                Arrays.asList("a", "b", "c").iterator(), Arrays.asList("d", "e").iterator()));

        iterator.finish();
        String[] objects = Iterators.toArray(iterator, String.class);
        assertThat(objects, Matchers.arrayContaining("a", "b", "c", "d", "e"));
    }

    @Test
    public void testInputIsPassedThroughWithSecondMergeCall() throws Exception {
        PassThroughPagingIterator<String> iterator = new PassThroughPagingIterator<>();
        iterator.merge(Arrays.asList(
                Arrays.asList("a", "b", "c").iterator(), Arrays.asList("d", "e").iterator()));
        iterator.merge(Collections.singletonList(Arrays.asList("f", "g").iterator()));
        iterator.finish();
        String[] objects = Iterators.toArray(iterator, String.class);
        assertThat(objects, Matchers.arrayContaining("a", "b", "c", "d", "e", "f", "g"));
    }
}