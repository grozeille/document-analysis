package grozeille;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by Mathias on 21/02/2015.
 */
public class SparkUtils {
    public static void mergeFiles(File parentDirectory, String fileName) throws IOException {

        Collection<File> outputFiles = FileUtils.listFiles(parentDirectory, FileFilterUtils.prefixFileFilter("part-"), DirectoryFileFilter.DIRECTORY);

        IOCopier.joinFiles(new File(parentDirectory, fileName), outputFiles.toArray(new File[0]));
    }
}
