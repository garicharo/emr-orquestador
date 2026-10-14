package com.orquestador;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parents come out before children, even when the input list is backwards.
 * A missing parent or a cycle fails the batch instead of running the child alone.
 */
public final class TaskGraph {

    private TaskGraph() {
    }

    public static List<Task> order(List<Task> tasks) {
        Map<String, Task> byId = new LinkedHashMap<>();
        for (Task task : tasks) {
            if (byId.putIfAbsent(task.id(), task) != null) {
                throw new IllegalArgumentException("duplicate id " + task.id());
            }
        }
        List<Task> ordered = new ArrayList<>();
        Set<String> done = new HashSet<>();
        Set<String> stack = new HashSet<>();
        for (Task task : byId.values()) {
            visit(task.id(), byId, ordered, done, stack);
        }
        return List.copyOf(ordered);
    }

    private static void visit(
            String id,
            Map<String, Task> byId,
            List<Task> ordered,
            Set<String> done,
            Set<String> stack) {
        if (done.contains(id)) {
            return;
        }
        if (!stack.add(id)) {
            throw new IllegalStateException("cycle at " + id);
        }
        Task task = byId.get(id);
        if (task == null) {
            throw new IllegalArgumentException("missing task " + id);
        }
        String parentId = task.dependsOn();
        if (parentId != null) {
            if (!byId.containsKey(parentId)) {
                throw new IllegalArgumentException("missing dependency " + parentId + " for " + id);
            }
            visit(parentId, byId, ordered, done, stack);
        }
        stack.remove(id);
        done.add(id);
        ordered.add(task);
    }
}
