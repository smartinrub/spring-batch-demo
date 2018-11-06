package org.smartinrub.springbatchdemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.ReferenceJobFactory;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/batch")
public class JobLauncherController {

    private final Job hashJob;
    private final Job rollBackJob;
    private final JobOperator jobOperator;
    private final JobRepository jobRepository;
    private final JobRegistry jobRegistry;
    private JobExecution execution;

    @Autowired
    public JobLauncherController(Job hashJob, Job rollBackJob, JobOperator jobOperator, JobRepository jobRepository, JobRegistry jobRegistry) throws DuplicateJobException {
        this.hashJob = hashJob;
        this.rollBackJob = rollBackJob;
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.jobRegistry = jobRegistry;
        jobRegistry.register(new ReferenceJobFactory(hashJob));
        jobRegistry.register(new ReferenceJobFactory(rollBackJob));
    }

    @RequestMapping("/start")
    public String startMigrationJob() throws JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException {
        jobOperator.start(hashJob.getName(), "");
        return "Migration started!";

    }

    @RequestMapping("/continue")
    public String continueMigrationJob() {
        try {
            jobOperator.restart(execution.getJobId());
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException | JobRestartException | JobParametersInvalidException e) {
            log.error("Migration is not running");
            throw new RuntimeException(e);
        }
        return "Migration restarted!";
    }

    @RequestMapping("/stop")
    public String stopMigrationJob() {
        execution = jobRepository.getLastJobExecution(hashJob.getName(), new JobParameters());
        try {
            if (execution != null) {
                jobOperator.stop(execution.getJobId());
            }
            log.error("Migration is not running");
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
            log.error("Migration is not running");
            throw new RuntimeException(e);
        }
        return "Migration stopped!";
    }

    @RequestMapping("/rollback")
    public String rollBackJob() throws JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException {
        jobOperator.start(rollBackJob.getName(), "");
        return "Rollback started!";
    }

}
