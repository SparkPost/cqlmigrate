package uk.sky.cqlmigrate;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java8.util.stream.Stream;
import java8.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CqlPaths {

    private static final String BOOTSTRAP_CQL = "bootstrap.cql";
    private static final String CQL_FILE_FILTER = "*.cql";
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlMigratorImpl.class);


    private final ImmutableSortedMap<String, Path> sortedCqlPaths;

    public CqlPaths(Map<String, Path> paths) {
        this.sortedCqlPaths = ImmutableSortedMap.copyOf(paths);
    }

    static CqlPaths create(Collection<Path> directories) {
        Map<String, Path> cqlPathsMap = new HashMap<>();

        StreamSupport.stream(directories)
                .forEach( directory -> {
                    try( DirectoryStream<Path> directoryStream = directoryStreamFromPath(directory)) {
                      for (Path path : directoryStream) {
                        addPathToMap(cqlPathsMap, path);
                      }
                    } catch(IOException e){
                      LOGGER.error("Error creating migrations {}", e);
                      throw Throwables.propagate(e);
                    }
                });

        return new CqlPaths(cqlPathsMap);
    }

    private static DirectoryStream<Path> directoryStreamFromPath(Path path) {
        try {
            return Files.newDirectoryStream(path, CQL_FILE_FILTER);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void applyInSortedOrder(Function function) {
        StreamSupport.stream(sortedCqlPaths.keySet())
                .filter(filename -> !filename.equals(BOOTSTRAP_CQL))
                .forEach(filename -> function.apply(filename, sortedCqlPaths.get(filename)));
    }

    public void applyBootstrap(Function function) {
        function.apply(BOOTSTRAP_CQL, sortedCqlPaths.get(BOOTSTRAP_CQL));
    }

    public interface Function {
        void apply(String filename, Path path);
    }

    private static void addPathToMap(Map<String, Path> paths, Path path) {
        String cqlFileName = path.getFileName().toString();
        if (paths.put(path.getFileName().toString(), path.toAbsolutePath()) != null) {
            throw new IllegalArgumentException(String.format("Multiple files with the same name: %s, %s", cqlFileName, path.toAbsolutePath()));
        }
    }
}
