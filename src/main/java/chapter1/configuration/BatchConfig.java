package chapter1.configuration;

import chapter1.batch.DecompressTasklet;
import chapter1.batch.JobCompletionNotificationListener;
import chapter1.batch.ProductFieldSetMapper;
import chapter1.batch.ProductJdbcItemWriter;
import chapter1.model.Product;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/*
    todo : best practice: when configuring a Spring Batch application,
     the infrastructure and job configuration should be in separate files.

     the infrastructure configuration file defines the
        job repository and data source beans;
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FieldSetMapper fieldSetMapper(){
        return new ProductFieldSetMapper();
    }

    // add scope -> Step      @Value("#{jobParameters['file.output']}") String output
    @StepScope
    @Bean
    public FlatFileItemReader<Product> reader(@Value("#{jobParameters['inputZip']}") String inputZip){
       return new FlatFileItemReaderBuilder<Product>()
                .name("productReader")
                .resource(new ClassPathResource(inputZip /*"#{jobParameters['inputZip']}"*/)) // "chapter1/data_txt"
                .delimited()
                .names(new String[]{"PRODUCT_ID","NAME","DESCRIPTION","PRICE"})
                .lineMapper(lineMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public LineMapper<Product> lineMapper() {
        final DefaultLineMapper<Product> defaultLineMapper = new DefaultLineMapper<>();
        final DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[] {"PRODUCT_ID","NAME","DESCRIPTION","PRICE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper());
        return defaultLineMapper;
    }

    // ######### Config writer #############################################
    @Bean
    ProductJdbcItemWriter writer(final DataSource dataSource){
        return new ProductJdbcItemWriter(dataSource);
    }

    // #{jobParameters['targetDirectory']
    // +jobParameters['targetFile']}
    // add scope -> Step
    @StepScope
    @Bean
    Resource resource(@Value("#{jobParameters['inputResource']}") String inputResource){
        return new ClassPathResource(inputResource); // chapter1/input.zip

    }

    @Bean
    @StepScope
    // @Scope(Step) -> To be able to refer to job parameters, a bean must use the Spring Batch step scope
    DecompressTasklet decompressTasklet(Resource resource,
                                        @Value("#{jobParameters['targetDirectory']}") String targetDirectory,
                                        @Value("#{jobParameters['targetFile']}") String targetFile){
        return new DecompressTasklet(resource,
                                    targetDirectory, // src/main/resources/chapter1/target
                                    targetFile); // "products.txt"
    }

    // ####### JOB config #############
    @Bean
    public Step step2(ProductJdbcItemWriter writer, FlatFileItemReader<Product> reader) {
        return stepBuilderFactory.get("readWriteProducts")
                .<Product, Product> chunk(100)// use between 10 and 200..
                .reader(reader)
                .writer(writer)
                // assuming you can live with skipping some records instead of failing the whole job, you can
                // change the job config to keep on reading when the reader throws a FlatFileParseException
                .faultTolerant()
                .skipLimit(10)
                .skip(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step step1(DecompressTasklet decompressTasklet) {
        return stepBuilderFactory.get("decompressTasklet")
                .tasklet(decompressTasklet)
                .build();
    }

    @Bean
    JobCompletionNotificationListener listener(JdbcTemplate jdbcTemplate){
        return new JobCompletionNotificationListener(jdbcTemplate);
    }

    // because by default the job uses a jobRepository bean
    @Bean
    public Job importProductJob(JobCompletionNotificationListener listener,  Step step1, Step step2) {
        return jobBuilderFactory.get("importProductJob")
                .incrementer(new RunIdIncrementer())
                 .listener(listener)
                 .flow(step1)
                 .next(step2)
                 .end()
                 .build();
    }

//    @Bean
//    public JobExecution jobExecution(Job job) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
//        return new SimpleJobLauncher().run(job, new JobParametersBuilder()
//                                                    .addString("inputZip", "chapter1/data_txt")
//                                                    .addString("inputResource", "chapter1/input.zip")
//                                                    .addString("targetDirectory", "src/main/resources/chapter1/target")
//                                                    .addString("targetFile", "products.txt")
//                                                     .toJobParameters());
//    }
}
