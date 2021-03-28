package com.sagnik.cdcProcessorKafka;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

@Slf4j
@AllArgsConstructor
public class ShutdownHook implements Runnable {
    private final ExecutorService executorService;
    private final Collection<RunningProcess> runningProcesses;

    @Override
    public void run() {
        log.info("Initiating graceful shutdown");
        initimateRunningProcessToStop();
        waitForRunningProcessesToStop();
        shutdownExecutorThreads();
        log.info("Graceful shutdown complete");
    }

    private void initimateRunningProcessToStop() {
        runningProcesses.forEach(RunningProcess::stop);
    }

    private void waitForRunningProcessesToStop() {
        runningProcesses.forEach(RunningProcess::waitForCompletion);
    }

    private void shutdownExecutorThreads() {
        executorService.shutdown();
    }
}
