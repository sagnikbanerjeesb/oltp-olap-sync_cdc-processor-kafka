package com.sagnik.cdcProcessorKafka;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {
    public static void main(String[] args) {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        final StudentCDCProcessor studentCDCProcessor = new StudentCDCProcessor();
        final CompletableFuture<Void> studentFuture = CompletableFuture.runAsync(studentCDCProcessor, executorService);
        final RunningProcess studentCDCProcess = new RunningProcess(studentCDCProcessor, studentFuture);

        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(executorService, List.of(studentCDCProcess))));
    }
}
