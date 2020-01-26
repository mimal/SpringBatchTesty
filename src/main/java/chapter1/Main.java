package chapter1;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Main implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    private Job jobs;

    @Override
    public void run(String... args) throws Exception {
        JobLauncher jobLauncher = applicationContext.getBean(JobLauncher.class);
        JobExecution execution = jobLauncher.run(
            jobs, new JobParametersBuilder()
                        .addString("inputZip", "chapter1/data_txt")
                        .addString("inputResource", "chapter1/input.zip")
                        .addString("targetDirectory", "src/main/resources/chapter1/target")
                        .addString("targetFile", "products.txt")
                        .addLong("uniqueness", System.nanoTime())
                        .toJobParameters());
    }
}
