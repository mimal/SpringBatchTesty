package exampl3;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.MissingFormatArgumentException;

/*
Our application uses a two-step Job that reads a CSV input file with structured book information and outputs books and book details.
 */
@SpringBootApplication
public class Main implements CommandLineRunner {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    @Qualifier("transformBooksRecords")
    private Job transformBooksRecordsJob;

    @Value("${file.input}")
    private String input;

    @Value("${file.output}")
    private String output;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        JobParametersBuilder paramsBuilder = new JobParametersBuilder();
        paramsBuilder.addString("file.input", input);
        paramsBuilder.addString("file.output", output);
        jobLauncher.run(transformBooksRecordsJob, paramsBuilder.toJobParameters());
    }
}
