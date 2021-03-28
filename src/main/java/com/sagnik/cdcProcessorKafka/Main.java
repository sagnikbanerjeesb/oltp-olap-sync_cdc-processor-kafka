package com.sagnik.cdcProcessorKafka;

import com.sagnik.cdcProcessorKafka.cdcProcessing.ContactCDCProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.StudentCDCProcessor;
import com.sagnik.cdcProcessorKafka.gracefulShutdown.RunningProcess;
import com.sagnik.cdcProcessorKafka.gracefulShutdown.ShutdownHook;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {
    public static void main(String[] args) {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        final StudentCDCProcessor studentCDCProcessor = new StudentCDCProcessor();
        final CompletableFuture<Void> studentProcessorFuture = CompletableFuture.runAsync(studentCDCProcessor, executorService);
        final RunningProcess studentCDCProcess = new RunningProcess(studentCDCProcessor, studentProcessorFuture);

        final ContactCDCProcessor contactCDCProcessor = new ContactCDCProcessor();
        final CompletableFuture<Void> contactProcessorFuture = CompletableFuture.runAsync(contactCDCProcessor, executorService);
        final RunningProcess contactCDCProcess = new RunningProcess(contactCDCProcessor, contactProcessorFuture);

        final ShutdownHook shutdownHook = new ShutdownHook(executorService, List.of(studentCDCProcess, contactCDCProcess));
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));
    }
}
