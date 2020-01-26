package chapter1.batch;


import chapter1.batch.config.DbConfig;
import chapter1.configuration.BatchConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import javax.sql.DataSource;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfig.class, DbConfig.class})
@Sql(scripts = "classpath:schema.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@EnableBatchProcessing
public class ImportProductsIntegrationTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {
        jdbcTemplate.update("delete from product");
        jdbcTemplate.update("insert into product "+
                        "(id,name,description,price) values(?,?,?,?)", "PR....214","Nokia 2610 Phone","",102.23
        );
    }

    @Test
    public void importProducts() throws Exception {
        int initial = jdbcTemplate.queryForObject("select count(1) from product", Integer.class);

        jobLauncher.run(
                job, new JobParametersBuilder()
                        .addString("inputZip", "chapter1/data_txt")
                        .addString("inputResource", "chapter1/input.zip")
                        .addString("targetDirectory", "src/main/resources/chapter1/target")
                        .addString("targetFile", "products.txt")
                        .addLong("timestamp", System.currentTimeMillis())
                        .toJobParameters());

        int nbOfNewProducts = 7;
        Assert.assertEquals(initial+nbOfNewProducts, (int) jdbcTemplate.queryForObject("select count(1) from product", Integer.class));
    }

    // @Test
    public void importProductsWithErrors() throws Exception {
        /*
        int initial = jdbcTemplate.queryForInt("select count(1) from product");
        jobLauncher.run(job, new JobParametersBuilder()
                .addString("inputResource",
                        "classpath:/input/products_with_errors.zip")
                .addString("targetDirectory", "./target/importproductsbatch/")
                .addString("targetFile","products.txt")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters()
        );
        int nbOfNewProducts = 6;
        Assert.assertEquals(
                initial+nbOfNewProducts,
                jdbcTemplate.queryForInt("select count(1) from product")
        );
         */
    }
}
