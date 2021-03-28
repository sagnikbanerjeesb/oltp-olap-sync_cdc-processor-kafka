package com.sagnik.cdcProcessorKafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
@Getter
public class RunningProcess {
    private final Stoppable process;
    private final CompletableFuture processFuture;

    public void stop() {
        process.stop();
    }

    @SneakyThrows
    public Object waitForCompletion() {
        return processFuture.get();
    }
}
