package com.sagnik.cdcProcessorKafka.cdcProcessing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.sagnik.cdcProcessorKafka.cdcProcessing.CDCOperation.*;

@AllArgsConstructor
@Builder
@ToString // TODO remove
public class ChangeEvent {
    public static final Set<CDCOperation> OPERATIONS_HAVING_RELEVANT_DATA_IN_AFTER = Set.of(CREATE, UPDATE, INITIAL_LOAD);
    public static final Set<CDCOperation> OPERATIONS_HAVING_RELEVANT_DATA_IN_BEFORE = Set.of(UPDATE, DELETE);

    @NonNull
    private final CDCOperation operation;
    @NonNull
    private final Map<String, Object> before;
    @NonNull
    private final Map<String, Object> after;
    @NonNull
    private final Instant timestamp;

    public boolean isInsertion() {
        return INITIAL_LOAD == operation || CREATE == operation;
    }

    public boolean isUpdate() {
        return UPDATE == operation;
    }

    public boolean isDeletion() {
        return DELETE == operation;
    }

    public Map<String, Object> currentValuesForColumns(Set<String> columns) {
        if (OPERATIONS_HAVING_RELEVANT_DATA_IN_AFTER.contains(operation)) {
            return extractGivenColumnsFromAfter(columns);
        } else {
            throw new UnsupportedOperationException(); // TODO
        }
    }

    public Map<String, Object> previousValuesForColumns(Set<String> columns) {
        if (OPERATIONS_HAVING_RELEVANT_DATA_IN_BEFORE.contains(operation)) {
            return extractGivenColumnsFromBefore(columns);
        } else {
            throw new UnsupportedOperationException(); // TODO
        }
    }

    public Set<String> changedColumns() {
        return after.entrySet().stream()
                .filter(entry -> {
                    Object beforeValue = before.get(entry.getKey());
                    if (entry.getValue() == null) return beforeValue != null;
                    return !entry.getValue().equals(beforeValue);
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private Map<String, Object> extractGivenColumnsFromAfter(Set<String> columns) {
        return after.entrySet()
                .stream()
                .filter(entry -> columns.contains(entry.getKey()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    // FIXME refactor
    private Map<String, Object> extractGivenColumnsFromBefore(Set<String> columns) {
        return before.entrySet()
                .stream()
                .filter(entry -> columns.contains(entry.getKey()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }
}
