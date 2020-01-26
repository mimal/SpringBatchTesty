package exampl3;

import exampl3.model.Book;
import exampl3.model.BookDetails;
import exampl3.model.BookRecord;
import exampl3.service.BookDetailsItemProcessor;
import exampl3.service.BookItemProcessor;
import exampl3.service.BookRecordFieldSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class Config {
    private static Logger LOGGER = LoggerFactory.getLogger(Config.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    private static final String[] TOKENS = {
            "bookname", "bookauthor", "bookformat", "isbn", "publishyear" };

    @Bean
    @StepScope
    public BookItemProcessor bookItemProcessor() {
        return new BookItemProcessor();
    }

    @Bean
    @StepScope
    public BookDetailsItemProcessor bookDetailsItemProcessor() {
        return new BookDetailsItemProcessor();
    }


    // StepScope -> this object will share its lifetime with StepExecution
    // This also allows us to inject dynamic values at runtime so that we can pass our input file from the JobParameters
    @Bean
    @StepScope
    public FlatFileItemReader<BookRecord> csvItemReader(@Value("#{jobParameters['file.input']}") String input) {
        FlatFileItemReaderBuilder<BookRecord> builder = new FlatFileItemReaderBuilder<>();
        FieldSetMapper<BookRecord> bookRecordFieldSetMapper = new BookRecordFieldSetMapper();
        LOGGER.info("Configuring reader to input {}", input);
        return builder
                .name("bookRecordItemReader")
                .resource(new FileSystemResource(input))
                .delimited()
                .names(TOKENS)
                .fieldSetMapper(bookRecordFieldSetMapper)
                .build();
    }

    @Bean(name = "transformBooksRecords")
    public Job transformBookRecords(Step step1, Step step2) throws IOException {
        // @formatter:off
        return jobBuilderFactory
                .get("transformBooksRecords")
                .flow(step1)
                //.next(step2)
                //.flow(step2)
                .end()
                .build();
        // @formatter:on
    }

    @Bean
    @StepScope
    public JsonFileItemWriter<Book> jsonItemWriter(@Value("#{jobParameters['file.output']}") String output) throws IOException {
        JsonFileItemWriterBuilder<Book> builder = new JsonFileItemWriterBuilder<>();
        JacksonJsonObjectMarshaller<Book> marshaller = new JacksonJsonObjectMarshaller<>();
        return builder
                .name("bookItemWriter")
                .jsonObjectMarshaller(marshaller)
                .resource(new FileSystemResource(output))
                .build();
    }

    @Bean
    public Step step1(ItemReader<BookRecord> csvItemReader, ItemWriter<Book> jsonItemWriter) throws IOException {
        return stepBuilderFactory
                .get("step1")
                .<BookRecord, Book> chunk(3)
                .reader(csvItemReader)
                .processor(bookItemProcessor())
                .writer(jsonItemWriter)
                .build();
    }

    //@Bean
    public Step step2(
            ItemReader<BookRecord> csvItemReader, ItemWriter<BookDetails> listItemWriter) {
        return stepBuilderFactory
                .get("step2")
                .<BookRecord, BookDetails> chunk(3)
                .reader(csvItemReader)
                .processor(bookDetailsItemProcessor())
                .writer(listItemWriter)
                .build();
    }
}
