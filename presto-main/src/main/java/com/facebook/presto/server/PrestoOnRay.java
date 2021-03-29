/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.ray;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;

import com.facebook.presto.server.PrestoServer;

public class PrestoOnRay {
    public static int square(int x)
    {
        return x * x;
    }

    public static class Counter
    {
        private int value;
        Counter()
        {
            value = 0;
        }

        public void increment()
        {
            this.value += 1;
        }

        public int read()
        {
            return this.value;
        }
    }

    public static void main(String[] args) {
        Ray.init();
        {
            List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
            // Invoke the `square` method 4 times remotely as Ray tasks.
            // The tasks will run in parallel in the background.
            for (int i = 0; i < 4; i++) {
                objectRefList.add(Ray.task(PrestoOnRay::square, i).remote());
            }
            // Get the actual results of the tasks with `get`.
            System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
        }

        {
            List<ActorHandle<Counter>> counters = new ArrayList<>();
            // Create 4 actors from the `Counter` class.
            // They will run in remote worker processes.
            for (int i = 0; i < 4; i++) {
                counters.add(Ray.actor(Counter::new).remote());
            }

            // Invoke the `increment` method on each actor.
            // This will send an actor task to each remote actor.
            for (ActorHandle<Counter> counter : counters) {
                counter.task(Counter::increment).remote();
            }
            // Invoke the `read` method on each actor, and print the results.
            List<ObjectRef<Integer>> objectRefList = counters.stream()
                .map(counter -> counter.task(Counter::read).remote())
                .collect(Collectors.toList());
            System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
        }
        new PrestoServer().run();
    }
}
