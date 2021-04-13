package com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors.contact;

import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCKafkaProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.ChangeEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.Set;

// FIXME: This class is very similar to StudentCDCProcessor. Can be generalised.
@Slf4j
public class ContactCDCProcessor extends CDCKafkaProcessor {
    private static final String TOPIC = "oltp-olap-sync.public.contact";
    private static final String PRIMARY_KEY = "contact_id";
    public static final String JOIN_COLUMN = "student_id";

    // FIXME: Read query doesn't belong here as this is not tightly coupled to contact domain (except the where clause)
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
                    "  contact.contact_id = ?";

    // FIXME: Write query doesn't belong here as its not coupled to contact
    private static final String WRITE_QUERY =
                    "INSERT INTO student_contact (student_id, contact_id, name, contact_number) " +
                    "  VALUES (?, ?, ?, ?) " +
                    "ON CONFLICT(student_id, contact_id) " +
                    "DO " +
                    "  UPDATE SET name = EXCLUDED.name, contact_number = EXCLUDED.contact_number";

    private static final String DELETE_BY_PK_QUERY =
                    "DELETE FROM student_contact " +
                    "  WHERE contact_id = ?";

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    public ContactCDCProcessor(JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        super(TOPIC);
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    @Override
    protected void processCDCEvent(ChangeEvent changeEvent) {
        log.info("PROCESSING Contact CDC Event: {}", changeEvent.toString());

        if (changeEvent.isUpdate() && changeEvent.changedColumns().contains(JOIN_COLUMN)) {
            final long previousContactId = getPreviousContactId(changeEvent);
            final int affectedRows = targetJdbcTemplate.update(DELETE_BY_PK_QUERY, previousContactId);
            log.info("Deleted {} rows", affectedRows);
            final long currentContactId = getCurrentContactId(changeEvent);
            sourceJdbcTemplate.query(READ_QUERY, this::pushToOlap, currentContactId);
        } else if (changeEvent.isInsertion() || changeEvent.isUpdate()) {
            final long contactId = getCurrentContactId(changeEvent);
            sourceJdbcTemplate.query(READ_QUERY, this::pushToOlap, contactId);
        } else if (changeEvent.isDeletion()) {
            final long contactId = getPreviousContactId(changeEvent);
            final int affectedRows = targetJdbcTemplate.update(DELETE_BY_PK_QUERY, contactId);
            log.info("Deleted {} rows", affectedRows);
        }
    }

    private long getCurrentContactId(ChangeEvent changeEvent) {
        final long contactId;
        final Object contactIdObj = changeEvent.currentValuesForColumns(Set.of(PRIMARY_KEY)).get(PRIMARY_KEY); // FIXME
        if (contactIdObj instanceof Integer) {
            contactId = (Integer) contactIdObj;
        } else {
            contactId = (Long) contactIdObj;
        }
        return contactId;
    }

    // FIXME refactor
    private long getPreviousContactId(ChangeEvent changeEvent) {
        final long contactId;
        final Object contactIdObj = changeEvent.previousValuesForColumns(Set.of(PRIMARY_KEY)).get(PRIMARY_KEY); // FIXME
        if (contactIdObj instanceof Integer) {
            contactId = (Integer) contactIdObj;
        } else {
            contactId = (Long) contactIdObj;
        }
        return contactId;
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
