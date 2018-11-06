package org.smartinrub.springbatchdemo;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class JobMigrationListener extends JobExecutionListenerSupport {

    private static final String SQL_SELECT_CREDENTIALS = "SELECT * FROM credentials";
    private static final String SQL_SELECT_CREDENTIALS_BACKUP = "SELECT * FROM credentials_backup";
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("HASHING IS DONE!!!");

            jdbcTemplate.query(SQL_SELECT_CREDENTIALS,
                    (rs, row) -> new Credentials(
                            rs.getString(1),
                            rs.getString(2))
            ).forEach(credentials -> log.info("New credentials -> " + credentials));

            jdbcTemplate.query(SQL_SELECT_CREDENTIALS_BACKUP,
                    (rs, row) -> new Credentials(
                            rs.getString(1),
                            rs.getString(2))
            ).forEach(credentials -> log.info("Backup credentials -> " + credentials));
        }
    }
}
