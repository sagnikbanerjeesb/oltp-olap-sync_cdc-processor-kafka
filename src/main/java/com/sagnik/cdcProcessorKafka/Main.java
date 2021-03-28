package com.sagnik.cdcProcessorKafka;

import com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors.contact.ContactCDCProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors.student.StudentCDCProcessor;
import com.sagnik.cdcProcessorKafka.gracefulShutdown.RunningProcess;
import com.sagnik.cdcProcessorKafka.gracefulShutdown.ShutdownHook;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {
    public static void main(String[] args) {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        final JdbcTemplate sourceJdbcTemplate = oltpJdbcTemplate();
        final JdbcTemplate targetJdbcTemplate = olapJdbcTemplate();

        final StudentCDCProcessor studentCDCProcessor = new StudentCDCProcessor(sourceJdbcTemplate, targetJdbcTemplate);
        final CompletableFuture<Void> studentProcessorFuture = CompletableFuture.runAsync(studentCDCProcessor, executorService);
        final RunningProcess studentCDCProcess = new RunningProcess(studentCDCProcessor, studentProcessorFuture);

        final ContactCDCProcessor contactCDCProcessor = new ContactCDCProcessor(sourceJdbcTemplate, targetJdbcTemplate);
        final CompletableFuture<Void> contactProcessorFuture = CompletableFuture.runAsync(contactCDCProcessor, executorService);
        final RunningProcess contactCDCProcess = new RunningProcess(contactCDCProcessor, contactProcessorFuture);

        final ShutdownHook shutdownHook = new ShutdownHook(executorService, List.of(studentCDCProcess, contactCDCProcess));
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));
    }

    private static DataSource oltpDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/pg");
        dataSource.setUsername("pg");
        dataSource.setPassword("pg");

        return dataSource;
    }

    private static JdbcTemplate oltpJdbcTemplate() {
        return new JdbcTemplate(oltpDataSource());
    }

    private static DataSource olapDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5433/pg");
        dataSource.setUsername("pg");
        dataSource.setPassword("pg");

        return dataSource;
    }

    private static JdbcTemplate olapJdbcTemplate() {
        return new JdbcTemplate(olapDataSource());
    }
}
