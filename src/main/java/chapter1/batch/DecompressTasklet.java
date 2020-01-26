package chapter1.batch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.ZipInputStream;


/*
    Spring Batch provides an extension point to handle processing in a batch process step: the Tasklet
 */
public class DecompressTasklet implements Tasklet {

    // archive file, the name of the directory to which the file is decompressed, and the
    //name of the output file.
    private Resource inputResource;
    private String targetDirectory;
    private String targetFile;

    public DecompressTasklet(Resource inputResource, String targetDirectory, String targetFile) {
        this.inputResource = inputResource;
        this.targetDirectory = targetDirectory;
        this.targetFile = targetFile;
    }

    /*
    In the execute method, you open a stream to the archive file, create the
    target directory if it doesn’t exist , and use the Java API to decompress the ZIP archive

    Note that the FileUtils and IOUtils classes from the Apache Commons
    IO project are used to create the target directory and copy the ZIP entry content to the
    target file (Apache Commons IO provides handy utilities to deal with files and directories)
     */
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        // opens archive
        ZipInputStream zipInputStream = new ZipInputStream(new BufferedInputStream(inputResource.getInputStream()));

        File targetDirectoryAsFile = new File(targetDirectory);
        // create target directory if absent
        if(!targetDirectoryAsFile.exists()){
            FileUtils.forceMkdir(targetDirectoryAsFile);
        }

        File target = new File(targetDirectory, targetFile);
        BufferedOutputStream dest = null;
        while (zipInputStream.getNextEntry() != null){
            if(!target.exists()){
                target.createNewFile();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(target);
            dest = new BufferedOutputStream(fileOutputStream);
            IOUtils.copy(zipInputStream, dest);
            dest.flush();
            dest.close();
        }
        zipInputStream.close();
        if(!target.exists()) {
            throw new IllegalStateException("Could not decompress anything from the archive!");
        }
        return RepeatStatus.FINISHED; // you return the FINISHED constant from the RepeatStatus enumeration to
                                     // notify Spring Batch that the tasklet finished
    }
}
