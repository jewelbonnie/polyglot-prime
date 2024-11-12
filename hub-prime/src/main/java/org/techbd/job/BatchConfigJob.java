package org.techbd.job;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.techbd.tasklet.ZipProcessingTasklet;

import org.slf4j.Logger;

@Configuration
public class BatchConfigJob {

    private final JobRepository jobRepository;
    private final JobLauncher jobLauncher;
    private final PlatformTransactionManager transactionManager;
    private final ZipProcessingTasklet zipProcessingTasklet;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime now = LocalDateTime.now();

    private static final Logger logger = LoggerFactory.getLogger(BatchConfigJob.class);

    public BatchConfigJob(JobRepository jobRepository, JobLauncher jobLauncher,
            PlatformTransactionManager transactionManager, ZipProcessingTasklet zipProcessingTasklet) {
        this.jobRepository = jobRepository;
        this.jobLauncher = jobLauncher;
        this.transactionManager = transactionManager;
        this.zipProcessingTasklet = zipProcessingTasklet;
        System.out.println("***Job init Started at :" + now.format(formatter));
    }

    /**
     * Scheduled method to perform the job every 30 seconds.
     *
     * @throws Exception if any error occurs during job execution.
     */
    @Scheduled(cron = "0/30 * * * * ?")
    public void perform() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("JobID", System.currentTimeMillis())
                    .toJobParameters();

            var execution = jobLauncher.run(
                    zipProcessingJob(),
                    params);

            logger.info("Job finished with status: {}", execution.getStatus());
        } catch (Exception e) {
            logger.error("Error executing job: ", e);
        }
    }

    @Bean
    public Job zipProcessingJob() {
        return new JobBuilder("zipProcessingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(zipProcessingStep())
                .build();
    }

    @Bean
    public Step zipProcessingStep() {
        return new StepBuilder("zipProcessingStep", jobRepository)
                .tasklet(zipProcessingTasklet, transactionManager)
                .build();
    }
}
