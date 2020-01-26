package chapter1.batch.unit_testing;

import chapter1.batch.config.DbConfig;
import chapter1.configuration.BatchConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;

/*
    The components that are configured at runtime to be step- or job-scoped are tricky to test as standalone
    components unless you have a way to set the context as if they were in a step or job execution.
    That is the goal of the org.springframework.batch.test.StepScopeTestExecutionListener and
    org.springframework.batch.test.StepScopeTestUtils components in Spring Batch,
    as well as JobScopeTestExecutionListener and JobScopeTestUtils. --> https://www.toptal.com/spring/spring-batch-tutorial
 */

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchConfig.class, BatchTestConfiguration.class, DbConfig.class})
public class CustemTest {

    @Autowired
    private JobLauncherTestUtils testUtils;

    @Autowired
    private Job job;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void tst() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        final JobExecution result = testUtils.getJobLauncher().run(job, testUtils.getUniqueJobParameters());
        Assert.assertNotNull(result);
        Assert.assertEquals(BatchStatus.COMPLETED, result.getStatus());
    }
}
