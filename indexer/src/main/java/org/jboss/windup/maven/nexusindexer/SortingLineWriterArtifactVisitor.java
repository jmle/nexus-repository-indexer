package org.jboss.windup.maven.nexusindexer;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.maven.index.ArtifactInfo;

/**
 *
 *  @author <a href="http://ondra.zizka.cz/">Ondrej Zizka, zizka@seznam.cz</a>
 */
public class SortingLineWriterArtifactVisitor implements RepositoryIndexManager.ArtifactVisitor<Object>
{
    private static final Logger LOG = Logger.getLogger(SortingLineWriterArtifactVisitor.class.getName());
    private static final int BATCH_SIZE = 10000; // Process in batches to limit memory usage

    private final File outFile;
    private final Path tempDir;
    private final ArtifactFilter filter;
    private final ConcurrentLinkedQueue<String> currentBatch = new ConcurrentLinkedQueue<>();
    private final AtomicInteger batchCounter = new AtomicInteger(0);
    private final AtomicInteger artifactCounter = new AtomicInteger(0);


    public SortingLineWriterArtifactVisitor(File outFile, ArtifactFilter filter)
    {
        this.outFile = outFile;
        this.filter = filter;
        try
        {
            this.tempDir = Files.createTempDirectory("maven-indexer-batch");
            LOG.info("Created temporary directory for batching: " + tempDir);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed creating temporary directory for batching: " + ex.getMessage(), ex);
        }
    }


    @Override
    public void visit(ArtifactInfo artifact)
    {
        if (!this.filter.accept(artifact))
            return;
            
        // G:A:[P:[C:]]V
        // Unfortunately, G:A:::V leads to empty strings instead of nulls, see FORGE-2230.
        StringBuilder line = new StringBuilder();
        // Add to the text file
        line.append(artifact.getSha1()).append(' ');
        line.append(artifact.getGroupId()).append(":");
        line.append(artifact.getArtifactId()).append(":");
        // if (info.getPackaging() != null)
        line.append(artifact.getPackaging()).append(":");
        // if (info.getClassifier() != null)
        line.append(artifact.getClassifier()).append(":");
        line.append(artifact.getVersion());
        line.append("\n");
        
        currentBatch.add(line.toString());
        
        // Process batch when it reaches the batch size
        if (currentBatch.size() >= BATCH_SIZE) {
            synchronized (this) {
                if (currentBatch.size() >= BATCH_SIZE) {
                    processBatch();
                }
            }
        }
        
        int count = artifactCounter.incrementAndGet();
        if (count % 50000 == 0) {
            LOG.info("Processed " + count + " artifacts");
        }
    }


    public Object done()
    {
        try {
            // Process any remaining items in the current batch
            if (!currentBatch.isEmpty()) {
                processBatch();
            }
            
            // Merge all sorted batch files into the final output
            mergeSortedFiles();
            
            // Clean up temporary directory
            Files.walk(tempDir)
                .map(Path::toFile)
                .forEach(File::delete);
            Files.deleteIfExists(tempDir);
            
            LOG.info("Completed processing " + artifactCounter.get() + " artifacts");
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed finalizing sorted output: " + ex.getMessage(), ex);
        }
        return null;
    }
    
    private void processBatch() 
    {
        if (currentBatch.isEmpty()) {
            return;
        }
        
        try {
            // Drain current batch to a list, sort it, and write to temp file
            List<String> batchItems = new java.util.ArrayList<>();
            String item;
            while ((item = currentBatch.poll()) != null) {
                batchItems.add(item);
            }
            
            Collections.sort(batchItems);
            
            int batchNum = batchCounter.incrementAndGet();
            Path batchFile = tempDir.resolve("batch_" + batchNum + ".txt");
            
            try (FileWriter writer = new FileWriter(batchFile.toFile())) {
                for (String line : batchItems) {
                    writer.write(line);
                }
            }
            
            LOG.info("Wrote batch " + batchNum + " with " + batchItems.size() + " items");
            
            // Clear the batch list to free memory
            batchItems.clear();
        }
        catch (IOException ex) {
            throw new RuntimeException("Failed processing batch: " + ex.getMessage(), ex);
        }
    }
    
    private void mergeSortedFiles() throws IOException 
    {
        List<Path> batchFiles = Files.list(tempDir)
            .filter(path -> path.getFileName().toString().startsWith("batch_"))
            .sorted()
            .collect(java.util.stream.Collectors.toList());
            
        if (batchFiles.isEmpty()) {
            LOG.warning("No batch files to merge");
            return;
        }
        
        LOG.info("Merging " + batchFiles.size() + " batch files into " + outFile);
        
        // Simple merge approach - since individual files are sorted, 
        // we can use a priority queue for efficient merging
        try (FileWriter finalWriter = new FileWriter(outFile)) {
            java.util.PriorityQueue<String> allLines = new java.util.PriorityQueue<>();
            
            // Read all lines from all batch files
            for (Path batchFile : batchFiles) {
                List<String> lines = Files.readAllLines(batchFile);
                allLines.addAll(lines);
            }
            
            // Write sorted lines to final output
            while (!allLines.isEmpty()) {
                finalWriter.write(allLines.poll());
            }
        }
    }



}
