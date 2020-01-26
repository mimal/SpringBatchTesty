package BatchProcessingLargeDataSets.configuration;

import BatchProcessingLargeDataSets.dao.entity.Voltage;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // configuring ItemReader -> reading data from CSV file
    @Bean
    public FlatFileItemReader<Voltage> reader() {
        return new FlatFileItemReaderBuilder<Voltage>()
                .name("voltItemReader")
                .resource(new ClassPathResource("Volts.csv"))
                .delimited()
                .names(new String[]{"volt", "time"})
                .lineMapper(lineMapper())  // map lines from file to domain object
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Voltage>() {{ // fieldSetMapper -> interface to map data obtained from a fieldset
                    setTargetType(Voltage.class);                           // to an object
                }})
                .build();
    }

    @Bean
    public LineMapper<Voltage> lineMapper() {
        final DefaultLineMapper<Voltage> defaultLineMapper = new DefaultLineMapper<>();
        final DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(";");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[] {"volt","time"});
        final VoltageFieldSetMapper fieldSetMapper = new VoltageFieldSetMapper();
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper((FieldSetMapper<Voltage>) fieldSetMapper);
        return defaultLineMapper;
    }

    // Once the data is read, this processor is used for processing the data such as data conversion, aplying business logic and so on
    @Bean
    public VoltageProcessor processor(){
        return new VoltageProcessor();
    }

    // Once the data is processed, the data needs to be stored in a database as per our requirement
    @Bean
    public JdbcBatchItemWriter<Voltage> writer(final DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Voltage>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO voltage (volt, time) VALUES (:volt, :time)")
                .dataSource(dataSource)
                .build();
    }


    // We will now define a Step which will contain a reader, processor and writer
    @Bean
    public Step step1(JdbcBatchItemWriter<Voltage> writer) {
        return stepBuilderFactory.get("step1").
                    <Voltage, Voltage> chunk(10)
                    .reader(reader())
                    .processor(processor())
                    .writer(writer)
                    .build();
    }

    @Bean
    public Job importVoltageJob(NotificationLIstener listener, Step step1) {
        return jobBuilderFactory.get("importVoltageJob")
                                .incrementer(new RunIdIncrementer())
                                .listener(listener)
                                .flow(step1)
                                .end()
                                .build();
    }
}
