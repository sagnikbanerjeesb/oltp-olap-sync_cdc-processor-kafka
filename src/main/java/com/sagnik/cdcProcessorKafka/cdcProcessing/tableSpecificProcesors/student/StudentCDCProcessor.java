package com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors.student;

import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCKafkaProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.ChangeEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.Set;

@Slf4j
public class StudentCDCProcessor extends CDCKafkaProcessor {
    private static final String TOPIC = "oltp-olap-sync.public.student";
    private static final String PRIMARY_KEY = "student_id";

    // FIXME: Read query doesn't belong here as this is not tightly coupled to student domain (except the where clause)
    private static final String READ_QUERY =
                    "SELECT " +
                    "  student.student_id as student_id, " +
                    "  contact.contact_id as contact_id, " +
                    "  student.name as name, " +
                    "  contact.contact_number as contact_number " +
                    "FROM student " +
                    "INNER JOIN contact " +
                    "  ON student.student_id = contact.student_id " +
                    "WHERE " +
                    "  student.student_id = ?";

    // FIXME: Write query doesn't belong here as its not coupled to student
    private static final String WRITE_QUERY =
                    "INSERT INTO student_contact (student_id, contact_id, name, contact_number) " +
                    "  VALUES (?, ?, ?, ?)";


    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    public StudentCDCProcessor(JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        super(TOPIC);
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    @Override
    protected void processCDCEvent(ChangeEvent changeEvent) {
        if (changeEvent.isInsertion()) {
            log.info("PROCESSING Student CDC Event: {}", changeEvent.toString());

            final long studentId = getStudentId(changeEvent);

            sourceJdbcTemplate.query(READ_QUERY, this::pushToOlap, studentId);
        }
    }

    private long getStudentId(ChangeEvent changeEvent) {
        final long studentId;
        final Object studentIdObj = changeEvent.currentValuesForColumns(Set.of(PRIMARY_KEY)).get(PRIMARY_KEY); // FIXME
        if (studentIdObj instanceof Integer) {
            studentId = (Integer) studentIdObj;
        } else {
            studentId = (Long) studentIdObj;
        }
        return studentId;
    }

    @SneakyThrows
    private void pushToOlap(ResultSet rs) {
        final int affectedRows = targetJdbcTemplate.update(WRITE_QUERY,
                rs.getLong("student_id"),
                rs.getLong("contact_id"),
                rs.getString("name"),
                rs.getString("contact_number"));

        log.info("Inserted {} rows", affectedRows);
    }
}
