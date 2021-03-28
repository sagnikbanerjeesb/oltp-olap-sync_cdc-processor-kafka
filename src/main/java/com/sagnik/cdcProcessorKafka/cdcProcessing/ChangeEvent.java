package com.sagnik.cdcProcessorKafka.cdcProcessing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
@Builder
@ToString // TODO remove
public class ChangeEvent {
    public static final Set<CDCOperation> OPERATIONS_HAVING_RELEVANT_DATA_IN_AFTER = Set.of(CDCOperation.CREATE, CDCOperation.UPDATE, CDCOperation.INITIAL_LOAD);

    @NonNull
    private final CDCOperation operation;
    @NonNull
    private final Map<String, Object> before;
    @NonNull
    private final Map<String, Object> after;
    @NonNull
    private final Instant timestamp;

    public Map<String, Object> currentValuesForColumns(Set<String> columns) {
        if (OPERATIONS_HAVING_RELEVANT_DATA_IN_AFTER.contains(operation)) {
            return extractGivenColumnsFromAfter(columns);
        } else {
            throw new UnsupportedOperationException(); // TODO
        }
    }

    private Map<String, Object> extractGivenColumnsFromAfter(Set<String> columns) {
        return after.entrySet()
                .stream()
                .filter(entry -> columns.contains(entry.getKey()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }
}