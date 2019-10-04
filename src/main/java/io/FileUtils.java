package io;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class FileUtils {

    /**
     * creates a directory at the given path
     *
     * @param path directory path
     * @return returns the path, if successful, null otherwise
     */
    public static Path createDirectory(Path path) {
        try {
            if (!path.toFile().exists()) {
                if (path.toFile().mkdir()) {
                    return path;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * creates directories recursively for a given file path
     *
     * @param path path to a file, where the directories should be created
     */
    public static void createDirectoryRecursively(Path root, Path path) {

        // convert to path first to have correct separators
        String rootPathAsString = root.toString();
        String deltaPath = path.toString().replace(rootPathAsString, "");
        if (deltaPath.charAt(0) == '\\' || deltaPath.charAt(0) == '/') {
            // remove first separator
            deltaPath = deltaPath.substring(1);
        }


        // loop through sub-folders in the git folder and create them if not existent
        Iterator<Path> it = Paths.get(deltaPath).iterator();
        while (it.hasNext()) {

            Path nextLevel = it.next();

            if (!it.hasNext()) {
                // should be the file now
                return;
            }

            root = Paths.get(root.toString(), nextLevel.toString());

            if (!root.toFile().exists()) {
                FileUtils.createDirectory(root);
            }
        }

    }

    /**
     * takes a path and returns the last component (file name or last directory)
     *
     * @param path path to be evaluated
     * @return last component
     */
    public static String getLastSegmentOfPath(Path path) {
        return getLastSegmentOfPath(path.toString());
    }


    /**
     * takes a path and returns the last component (file name or last directory)
     *
     * @param path path to be evaluated
     * @return last component
     */
    private static String getLastSegmentOfPath(String path) {
        char separator = '/';
        if (path.contains("\\")) {
            separator = '\\';
        }
        int lastIndex = path.lastIndexOf(separator) + 1;
        return path.substring(lastIndex);
    }




}
