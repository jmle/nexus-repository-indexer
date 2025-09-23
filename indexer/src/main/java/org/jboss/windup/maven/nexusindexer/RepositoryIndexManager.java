package org.jboss.windup.maven.nexusindexer;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.ArtifactContext;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.DefaultIndexer;
import org.apache.maven.index.DefaultIndexerEngine;
import org.apache.maven.index.DefaultQueryCreator;
import org.apache.maven.index.DefaultSearchEngine;
import org.apache.maven.index.Field;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.IteratorSearchRequest;
import org.apache.maven.index.IteratorSearchResponse;
import org.apache.maven.index.MAVEN;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.creator.JarFileContentsIndexCreator;
import org.apache.maven.index.creator.MavenArchetypeArtifactInfoIndexCreator;
import org.apache.maven.index.creator.MavenPluginArtifactInfoIndexCreator;
import org.apache.maven.index.creator.MinimalArtifactInfoIndexCreator;
import org.apache.maven.index.creator.OsgiArtifactIndexCreator;
import org.apache.maven.index.expr.SourcedSearchExpression;
import org.apache.maven.index.incremental.DefaultIncrementalHandler;
import org.apache.maven.index.incremental.IncrementalHandler;
import org.apache.maven.index.packer.DefaultIndexPacker;
import org.apache.maven.index.packer.IndexPacker;
import org.apache.maven.index.packer.IndexPackingRequest;
import org.apache.maven.index.updater.DefaultIndexUpdater;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdateSideEffect;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.providers.http.HttpWagon;
import org.apache.maven.wagon.repository.Repository;
import org.jboss.forge.addon.dependencies.DependencyRepository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;


/**
 * Downloads Maven index from given repository and produces a list of all artifacts, using this format: "SHA G:A:V[:C]".
 *
 * @author Ondrej Zizka, ozizka at redhat.com
 * @author <a href="mailto:lincolnbaxter@gmail.com">Lincoln Baxter, III</a>
 */
public class RepositoryIndexManager implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(RepositoryIndexManager.class.getName());

    public enum OutputFormat {
        TEXT,
        LUCENE
    }

    public static final String LUCENE_SUBDIR_CHECKSUMS = "lucene";

    private final File indexDirectory;

    private final Indexer indexer;
    private final IndexUpdater indexUpdater;
    private final Wagon httpWagon;
    private final IndexingContext context;
    final IndexPacker packer;

    private final File localCache;
    private final File indexDir;

    /**
     * Download the index for the given {@link DependencyRepository} and store the results at the specified output {@link File}
     * directory.
     */
    public static void generateMetadata(DependencyRepository repository, File indexDir, File outputDir, OutputFormat format) throws Exception
    {
        try (RepositoryIndexManager manager = new RepositoryIndexManager(indexDir, repository))
        {
            LOG.info("Downloading or updating index into " + indexDir.getPath());
            manager.downloadIndexAndUpdate();
            LOG.info("Writing selected Nexus index data to " + outputDir.getPath());
            manager.writeMetadataTo(outputDir, repository, format);
        }
    }

    /**
     * Download the index for the given {@link DependencyRepository}, retrieve the broken artifacts and update them with
     * the right values
     */
    public static void updateNexusIndex(DependencyRepository repository, File indexDir, File outputDir) throws Exception {
        try (RepositoryIndexManager manager = new RepositoryIndexManager(indexDir, repository)) {
            LOG.info("Downloading or updating index into " + indexDir.getPath());
            manager.downloadIndexAndUpdate();
            LOG.info("Update with fixes selected Nexus index data to " + outputDir.getPath());
            manager.updateNexusIndex(outputDir, repository);
        }
    }

    /**
     * Return <code>true</code> if metadata exists for the given {@link DependencyRepository} and output {@link File}
     * directory.
     */
    public static boolean metadataExists(DependencyRepository repository, File outputDir)
    {
        return getMetadataFile(repository, outputDir).exists();
    }

    /**
     * Get the metadata file for the given {@link DependencyRepository} and output {@link File} directory.
     */
    public static File getMetadataFile(DependencyRepository repository, File outputDir)
    {
        return new File(outputDir, repository.getId() + ".archive-metadata.txt");
    }

    /*
     * Make it clear that this should not be instantiated.
     */
    private RepositoryIndexManager(File indexDirectory, DependencyRepository repository) throws IOException
    {
        final boolean updateExistingIndex = true;

        this.indexDirectory = indexDirectory;

        this.indexer = new DefaultIndexer(
                new DefaultSearchEngine(),
                new DefaultIndexerEngine(),
                new DefaultQueryCreator()
        );

        IncrementalHandler incrementalHandler = new DefaultIncrementalHandler();
        List<IndexUpdateSideEffect> sideEffects = java.util.Collections.emptyList();
        indexUpdater = new DefaultIndexUpdater(incrementalHandler, sideEffects);

        packer = new DefaultIndexPacker(incrementalHandler);

        this.httpWagon = new HttpWagon();

        this.localCache = new File(this.indexDirectory, repository.getId() + "-cache");
        this.indexDir = new File(this.indexDirectory, repository.getId() + "-index");

        /*
         * https://maven.apache.org/maven-indexer/indexer-core/apidocs/index.html
         */
        List<IndexCreator> indexers = new ArrayList<>();
        indexers.add(new MinimalArtifactInfoIndexCreator());
        indexers.add(new OsgiArtifactIndexCreator());
        indexers.add(new MavenPluginArtifactInfoIndexCreator());
        indexers.add(new MavenArchetypeArtifactInfoIndexCreator());
        indexers.add(new JarFileContentsIndexCreator());
        this.context = this.indexer.createIndexingContext(
            repository.getId() + "Context", repository.getId(),
            this.localCache, this.indexDir,
            repository.getUrl(), null, true, updateExistingIndex, indexers);
    }

    private void downloadIndexAndUpdate() throws IOException
    {
        ResourceFetcher resourceFetcher = new SimpleResourceFetcher(httpWagon);
        IndexUpdateRequest updateRequest = new IndexUpdateRequest(this.context, resourceFetcher);
        updateRequest.setIncrementalOnly(false);
        updateRequest.setForceFullUpdate(false);
        IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex(updateRequest);
        if (updateResult.isFullUpdate())
            LOG.info("Fully updated index for repository [" + this.context.getId() + "] - [" + this.context.getRepositoryUrl() + "]");
        else
            LOG.info("Incrementally updated index for repository [" + this.context.getId() + "] - [" + this.context.getRepositoryUrl() + "]");
    }


    /**
     * Passes all artifacts from the index to the visitors.
     */
    private void writeMetadataTo(File outDir, DependencyRepository repository, OutputFormat outputFormat) throws IOException
    {
        outDir.mkdirs();

        // Maven repo index
        final IndexSearcher searcher = context.acquireIndexSearcher();
        final IndexReader reader = searcher.getIndexReader();
        Bits liveDocs = MultiBits.getLiveDocs(reader);

        final File textMetadataFile = getMetadataFile(repository, outDir);
        final List<RepositoryIndexManager.ArtifactVisitor<Object>> visitors = new ArrayList<>();

        if (outputFormat.equals(OutputFormat.TEXT))
        {
            SortingLineWriterArtifactVisitor writerVisitor = new SortingLineWriterArtifactVisitor(textMetadataFile, ArtifactFilter.LIBRARIES);
            visitors.add(writerVisitor);
        } else if (outputFormat.equals(OutputFormat.LUCENE))
        {
            LuceneIndexArtifactVisitor basicIndexerVisitor = new LuceneIndexArtifactVisitor(new File(outDir, LUCENE_SUBDIR_CHECKSUMS), ArtifactFilter.LIBRARIES);
            visitors.add(basicIndexerVisitor);
        }

        for (int i = 0; i < reader.maxDoc(); i++)
        {
            if (liveDocs != null && !liveDocs.get(i))
                continue;

            final Document doc = reader.document(i);
            final ArtifactInfo artifact = IndexUtils.constructArtifactInfo(doc, this.context);
            if (artifact == null){
                // This happens for documents which are not Artifact, e.g. Archetype etc.
                continue;
            }

            artifact.setSha1(StringUtils.lowerCase(artifact.getSha1()));
            artifact.setPackaging(StringUtils.defaultString(artifact.getPackaging()));
            artifact.setClassifier(StringUtils.defaultString(artifact.getClassifier()));

            for (ArtifactVisitor<Object> visitor : visitors)
            {
                try {
                    visitor.visit(artifact);
                }
                catch (Exception e) {
                    LOG.log(Level.SEVERE, "Failed processing " + artifact + " with " + visitor + "\n    " + e.getMessage());
                }
            }
        }

        final BooleanQuery missingArtifactsQuery = createMissingArtifactsQuery();

        final TotalHitCountCollector missingArtifactsQueryCountCollector = new TotalHitCountCollector();
        searcher.search(missingArtifactsQuery, missingArtifactsQueryCountCollector);
        final int artifactsCount = missingArtifactsQueryCountCollector.getTotalHits();
        LOG.log(Level.INFO, String.format("Found %d artifacts to be added in repository %s", artifactsCount, repository.getId()));
        final AtomicInteger managed = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);
        if (artifactsCount > 0) {
            Arrays.asList(searcher.search(missingArtifactsQuery, artifactsCount).scoreDocs)
                    .stream() // Use sequential stream to reduce memory pressure
                    .forEach(doc -> {
                                try {
                                    final ArtifactInfo wrongArtifactInfo = IndexUtils.constructArtifactInfo(searcher.doc(doc.doc), this.context);
                                    final String sha1 = ArtifactDownloader.getJarSha1(repository.getUrl(), wrongArtifactInfo);
                                    if (!ArtifactUtil.isArtifactAlreadyIndexed(indexer, this.context, sha1, wrongArtifactInfo)) {
                                        final ArtifactInfo artifactInfo = new ArtifactInfo(repository.getId(),
                                                wrongArtifactInfo.getGroupId(), wrongArtifactInfo.getArtifactId(),
                                                wrongArtifactInfo.getVersion(), StringUtils.defaultString(null), "jar");
                                        artifactInfo.setSha1(sha1);
                                        artifactInfo.setPackaging("jar");
                                        for (ArtifactVisitor<Object> visitor : visitors) {
                                            try {
                                                visitor.visit(artifactInfo);
                                            } catch (Exception e) {
                                                LOG.log(Level.SEVERE, String.format("Failed processing %s with %s%n    %s", artifactInfo, visitor, e.getMessage()));
                                            }
                                        }
                                        if (managed.incrementAndGet() % 5000 == 0)
                                        {
                                            LOG.log(Level.INFO, String.format("Managed %d/%d artifacts ", managed.get(), artifactsCount));
                                        }
                                    } else {
                                        LOG.log(Level.INFO, String.format("Dependency %s is NOT missing anymore in the source index", wrongArtifactInfo.getUinfo()));
                                    }
                                } catch (IOException e) {
                                    errors.incrementAndGet();
                                    LOG.log(Level.WARNING, String.format("Document %s management has failed%n    %s", doc, e.getMessage()));
                                }
                            }
                    );
            LOG.log(Level.INFO, String.format("Managed %d/%d artifacts with %d artifacts not managed for problems (check log above)", managed.get(), artifactsCount, errors.get()));
        }

        for (ArtifactVisitor<Object> visitor : visitors)
        {
            try {
                visitor.done();
            } catch (Exception e) {
                    LOG.log(Level.SEVERE, "Failed finishing " + visitor, e);
            }
        }
        this.context.releaseIndexSearcher(searcher);
    }

    // This query is created to address certain missing artifacts from the index.
    // See https://issues.redhat.com/browse/WINDUP-2765 and https://issues.sonatype.org/browse/OSSRH-60950
    private BooleanQuery createMissingArtifactsQuery() {

        // Query for searching all the docs in the index with the 'packaging' (aka the extension) that is 'module'
        // e.g. Lucene query "+g:org.springframework.boot +a:spring-boot-starter-tomcat  +v:2.3.* +p:module"
        // https://repo1.maven.org/maven2/org/springframework/boot/spring-boot-starter-web/2.3.2.RELEASE/
        final TermQuery artifactsWithModuleQuery = new TermQuery(new Term(ArtifactInfo.PACKAGING, "module"));

        // Query for searching all the docs in the index with the 'packaging' (aka the extension) that is 'pom.sha512'
        // e.g. Lucene query "+g:org.springdoc +a:springdoc-openapi-common +v:1.4.? +p:pom.sha512"
        // https://repo1.maven.org/maven2/org/springdoc/springdoc-openapi-common/1.4.3/
        // https://repo1.maven.org/maven2/org/apache/ant/ant-commons-logging/1.8.0/
        final TermQuery artifactsWithPomSha512Query = new TermQuery(new Term(ArtifactInfo.PACKAGING, "pom.sha512"));

        // Searches for artifacts with "bundle" packaging and no Symbolic Bundle Name to cater for
        // artifacts like https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind/2.12.3
        // See also https://issues.redhat.com/browse/WINDUP-3300
        final TermQuery artifactsWithBundleQuery = new TermQuery(new Term(ArtifactInfo.PACKAGING, "bundle"));
        final TermRangeQuery artifactsWithNoSymNameQuery = TermRangeQuery.newStringRange(ArtifactInfo.BUNDLE_SYMBOLIC_NAME, null, null, true, true);
        final BooleanQuery missingBundleArtifactsClause = new BooleanQuery.Builder()
                .add(artifactsWithBundleQuery, BooleanClause.Occur.MUST)
                .add(artifactsWithNoSymNameQuery, BooleanClause.Occur.MUST_NOT)
                .build();

        // Looks for hashless artifacts
        final BooleanQuery.Builder missingHashArtifactsQueryBuilder = new BooleanQuery.Builder();
        final TermQuery artifactsWithJarPackaging = new TermQuery(new Term(ArtifactInfo.PACKAGING, "jar"));
        final TermRangeQuery artifactsWithNoHashQuery = TermRangeQuery.newStringRange(ArtifactInfo.SHA1, null, null, true, true);
        missingHashArtifactsQueryBuilder.add(artifactsWithJarPackaging, BooleanClause.Occur.MUST);
        missingHashArtifactsQueryBuilder.add(artifactsWithNoHashQuery, BooleanClause.Occur.MUST_NOT);
        BooleanQuery missingHashArtifactsQuery = missingHashArtifactsQueryBuilder.build();

        final BooleanQuery missingArtifactsQuery = new BooleanQuery.Builder()
                .add(artifactsWithModuleQuery, BooleanClause.Occur.SHOULD)
                .add(artifactsWithPomSha512Query, BooleanClause.Occur.SHOULD)
                .add(new BooleanClause(missingBundleArtifactsClause, BooleanClause.Occur.SHOULD))
                .add(new BooleanClause(missingHashArtifactsQuery, BooleanClause.Occur.SHOULD))
                .build();

        return missingArtifactsQuery;
    }

    private void updateNexusIndex(File outputDir, DependencyRepository repository) throws IOException {
        outputDir.mkdirs();

        LOG.log(Level.INFO, "Creating fresh index to avoid schema compatibility issues");
        
        // Create a fresh temporary index to avoid schema conflicts
        File tempIndexDir = new File(outputDir, "temp-index-" + System.currentTimeMillis());
        tempIndexDir.mkdirs();
        
        IndexingContext tempContext = null;
        try {
            // Create fresh index context
            List<IndexCreator> indexers = new ArrayList<>();
            indexers.add(new MinimalArtifactInfoIndexCreator());
            indexers.add(new OsgiArtifactIndexCreator());
            indexers.add(new MavenPluginArtifactInfoIndexCreator());
            indexers.add(new MavenArchetypeArtifactInfoIndexCreator());
            indexers.add(new JarFileContentsIndexCreator());
            
            tempContext = this.indexer.createIndexingContext(
                repository.getId() + "TempContext", repository.getId(),
                new File(tempIndexDir, "cache"), new File(tempIndexDir, "index"),
                repository.getUrl(), null, true, false, indexers); // false = don't update existing
            
            final IndexingContext finalTempContext = tempContext;

            final BooleanQuery missingArtifactsQuery = new BooleanQuery.Builder()
                // we want "module" artifacts
                .add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "module" ) ), BooleanClause.Occur.SHOULD )
                // we want "pom.sha512" artifacts
                .add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "pom.sha512" ) ), BooleanClause.Occur.SHOULD )
                // we want main artifacts only (no classifier)
                .add( indexer.constructQuery( MAVEN.CLASSIFIER, new SourcedSearchExpression( Field.NOT_PRESENT ) ), BooleanClause.Occur.MUST_NOT ).build();
            final IteratorSearchRequest request = new IteratorSearchRequest( missingArtifactsQuery, Collections.singletonList(context));
            final IteratorSearchResponse response = indexer.searchIterator(request);
            final int artifactsCount = response.getTotalHitsCount();
            LOG.log(Level.INFO, String.format("Found %d artifacts to be fixed in repository '%s'", artifactsCount, repository.getId()));
            final AtomicInteger managed = new AtomicInteger(0);
            final AtomicInteger errors = new AtomicInteger(0);
            final List<ArtifactContext> artifactsToBeAdded = new ArrayList<>();
            final int BATCH_SIZE = 1000; // Process in smaller batches to limit memory usage
            
            StreamSupport.stream(response.spliterator(), false) // Use sequential stream to reduce memory pressure
                    .forEach(artifactInfo -> {
                        try {
                            final String sha1 = ArtifactDownloader.getJarSha1(repository.getUrl(), artifactInfo);
                            if (!ArtifactUtil.isArtifactAlreadyIndexed(indexer, finalTempContext, sha1, artifactInfo)) {
                                // Create corrected artifact info
                                ArtifactInfo correctedArtifact = new ArtifactInfo(repository.getId(),
                                        artifactInfo.getGroupId(), artifactInfo.getArtifactId(),
                                        artifactInfo.getVersion(), null, "jar");
                                correctedArtifact.setSha1(sha1);
                                correctedArtifact.setPackaging("jar");
                                correctedArtifact.setFileExtension("jar");
                                
                                artifactsToBeAdded.add(new ArtifactContext(null, null, null, correctedArtifact, null));
                                
                                // Process in batches to limit memory usage
                                if (artifactsToBeAdded.size() >= BATCH_SIZE) {
                                    try {
                                        indexer.addArtifactsToIndex(new ArrayList<>(artifactsToBeAdded), finalTempContext);
                                        artifactsToBeAdded.clear();
                                        LOG.log(Level.INFO, String.format("Added batch of %d artifacts to fresh index", BATCH_SIZE));
                                    } catch (IOException e) {
                                        LOG.log(Level.SEVERE, "Failed adding batch to fresh index: " + e.getMessage());
                                    }
                                }
                                
                                if (managed.incrementAndGet() % 5000 == 0)
                                {
                                    LOG.log(Level.INFO, String.format("Processed %d/%d artifacts ", managed.get(), artifactsCount));
                                }
                            } else {
                                LOG.log(Level.FINE, String.format("Dependency %s already exists in corrected form", artifactInfo.getUinfo()));
                            }
                        }
                        catch (IOException e) {
                            errors.incrementAndGet();
                            LOG.log(Level.WARNING, String.format("Document %s processing failed%n    %s", artifactInfo, e.getMessage()));
                        }
                    });
            
            // Process any remaining artifacts
            if (!artifactsToBeAdded.isEmpty()) {
                try {
                    indexer.addArtifactsToIndex(artifactsToBeAdded, finalTempContext);
                    LOG.log(Level.INFO, String.format("Added final batch of %d artifacts to fresh index", artifactsToBeAdded.size()));
                } catch (IOException e) {
                    LOG.log(Level.SEVERE, "Failed adding final batch to fresh index: " + e.getMessage());
                }
            }
            
            LOG.log(Level.INFO, String.format("Processed %d/%d artifacts with %d artifacts not processed due to errors", managed.get(), artifactsCount, errors.get()));
            
            // Copy existing good artifacts from original index to fresh index
            copyGoodArtifacts(finalTempContext, repository);
            
            LOG.log(Level.INFO, String.format("Fresh index created, now packing it in %s", outputDir));
            final IndexSearcher indexSearcher = finalTempContext.acquireIndexSearcher();
            try {
                final IndexPackingRequest indexPackingRequest = new IndexPackingRequest(finalTempContext, indexSearcher.getIndexReader(), outputDir);
                indexPackingRequest.setCreateChecksumFiles(true);
                indexPackingRequest.setCreateIncrementalChunks(true);
                packer.packIndex(indexPackingRequest);
                LOG.log(Level.INFO, "Fresh index successfully packed");
            } catch (IOException e) {
                LOG.log(Level.SEVERE, String.format("Cannot pack fresh index: %s", e.getMessage()));
                throw e;
            } finally {
                finalTempContext.releaseIndexSearcher(indexSearcher);
            }
            
        } finally {
            // Clean up temporary context and files
            if (tempContext != null) {
                try {
                    tempContext.close(false);
                    indexer.closeIndexingContext(tempContext, false);
                } catch (IOException e) {
                    LOG.log(Level.WARNING, "Failed to close temporary context: " + e.getMessage());
                }
            }
            
            // Clean up temp directory
            try {
                if (tempIndexDir.exists()) {
                    deleteRecursively(tempIndexDir);
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Failed to clean up temporary directory: " + e.getMessage());
            }
        }
    }
    
    private void copyGoodArtifacts(IndexingContext targetContext, DependencyRepository repository) throws IOException {
        LOG.log(Level.INFO, "Copying existing good artifacts to fresh index");
        
        // Copy all artifacts that don't need fixing
        final IndexSearcher searcher = context.acquireIndexSearcher();
        try {
            final IndexReader reader = searcher.getIndexReader();
            Bits liveDocs = MultiBits.getLiveDocs(reader);
            
            final AtomicInteger copiedCount = new AtomicInteger(0);
            final List<ArtifactContext> artifactsToAdd = new ArrayList<>();
            final int BATCH_SIZE = 1000;
            
            for (int i = 0; i < reader.maxDoc(); i++) {
                if (liveDocs != null && !liveDocs.get(i))
                    continue;

                final Document doc = reader.document(i);
                final ArtifactInfo artifact = IndexUtils.constructArtifactInfo(doc, this.context);
                if (artifact == null) {
                    continue;
                }

                // Only copy artifacts that don't need fixing (not module, not pom.sha512, etc.)
                if (!"module".equals(artifact.getPackaging()) && 
                    !"pom.sha512".equals(artifact.getPackaging()) &&
                    (artifact.getClassifier() == null || artifact.getClassifier().isEmpty())) {
                    
                    artifactsToAdd.add(new ArtifactContext(null, null, null, artifact, null));
                    
                    if (artifactsToAdd.size() >= BATCH_SIZE) {
                        indexer.addArtifactsToIndex(new ArrayList<>(artifactsToAdd), targetContext);
                        artifactsToAdd.clear();
                        LOG.log(Level.INFO, String.format("Copied batch of %d good artifacts", BATCH_SIZE));
                    }
                    
                    if (copiedCount.incrementAndGet() % 10000 == 0) {
                        LOG.log(Level.INFO, String.format("Copied %d good artifacts", copiedCount.get()));
                    }
                }
            }
            
            // Process remaining artifacts
            if (!artifactsToAdd.isEmpty()) {
                indexer.addArtifactsToIndex(artifactsToAdd, targetContext);
                LOG.log(Level.INFO, String.format("Copied final batch of %d good artifacts", artifactsToAdd.size()));
            }
            
            LOG.log(Level.INFO, String.format("Completed copying %d good artifacts to fresh index", copiedCount.get()));
        } finally {
            context.releaseIndexSearcher(searcher);
        }
    }
    
    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    @Override
    public void close() throws IOException
    {
        this.context.close(false);
        this.indexer.closeIndexingContext(this.context, false);
    }



    /**
     * Normal visitor pattern which also allows to call a method after finished and retrieve a resulting object.
     */
    public interface ArtifactVisitor<T>
    {
        void visit(ArtifactInfo artifact);
        public T done();
    }

    private static class SimpleResourceFetcher implements ResourceFetcher {
        private final Wagon wagon;

        public SimpleResourceFetcher(Wagon wagon) {
            this.wagon = wagon;
        }

        @Override
        public void connect(String id, String url) throws IOException {
            try {
                Repository repository = new Repository(id, url);
                wagon.connect(repository);
            } catch (Exception e) {
                throw new IOException("Failed to connect to repository: " + url, e);
            }
        }

        @Override
        public void disconnect() throws IOException {
            try {
                wagon.disconnect();
            } catch (Exception e) {
                throw new IOException("Failed to disconnect", e);
            }
        }

        @Override
        public InputStream retrieve(String name) throws IOException {
            File tempFile = null;
            try {
                tempFile = File.createTempFile("maven-index-", ".tmp");
                wagon.get(name, tempFile);
                
                // Create a custom InputStream that deletes the temp file when closed
                File finalTempFile = tempFile;
                return new FileInputStream(tempFile) {
                    @Override
                    public void close() throws IOException {
                        try {
                            super.close();
                        } finally {
                            if (finalTempFile.exists()) {
                                finalTempFile.delete();
                            }
                        }
                    }
                };
            } catch (Exception e) {
                // Clean up temp file if something went wrong
                if (tempFile != null && tempFile.exists()) {
                    tempFile.delete();
                }
                throw new IOException("Failed to retrieve: " + name, e);
            }
        }
    }


}
