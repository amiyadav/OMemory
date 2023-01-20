package org.amity.helper;

import org.amity.concurrency.error.DatabaseException;
import org.amity.storage.StorageManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

/**
 *
 */
public class FileAndDirHelper {

    public static final String USER_HOME = "user.home";
    public static final String DB_NAME = "OMemory";
    public static final String PARTITION = "partition";
    public static final String DEFAULT_DB_PATH = System.getProperty(USER_HOME) + "/" + DB_NAME;
    public static final String PARTITION_PATH = DEFAULT_DB_PATH + "/" + PARTITION + "-";

    private static void createPartitionDirectories(String dbDir) {
        IntStream.range(0, StorageManager.MAX_PARTITION_ALLOWED).forEachOrdered(i -> {
            String str = PARTITION_PATH + i;
            setupDirectory(str);
        });
    }

    private static boolean setupDirectory(String fileDir) {
        File dir = new File(fileDir);
        boolean initialized = dir.exists();
        if (!initialized) {
            if (!dir.mkdirs()) {
                throw new DatabaseException("failed to create directory " + fileDir);
            }
        } else if (!dir.isDirectory()) {
            throw new DatabaseException(fileDir + " is not a directory");
        }
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir.toPath())) {
            initialized = initialized && dirStream.iterator().hasNext();
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return initialized;
    }
}
