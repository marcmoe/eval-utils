package xyz.eval.io;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Y. Petrick
 */
public class FilenameFilters {

    public static FilenameFilter equal(final String... filenames) {
        return new FilenameFilter() {
            private final List<String> iFilenames = Arrays.asList(filenames);

            @Override
            public boolean accept(final File dir, final String name) {
                return iFilenames.contains(name);
            }
        };
    }
}
