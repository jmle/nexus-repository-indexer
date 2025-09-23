package org.jboss.windup.maven.nexusindexer;


import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.maven.index.ArtifactInfo;

/**
 * For each visited archive, this creates a Lucene document with these fields:
 *  sha1 groupId artifactId packaging classifier version
 *
 * @author <a href="http://ondra.zizka.cz/">Ondrej Zizka, zizka@seznam.cz</a>
 */
public class LuceneIndexArtifactVisitor implements RepositoryIndexManager.ArtifactVisitor<Object>
{
    private static final Logger LOG = Logger.getLogger(LuceneIndexArtifactVisitor.class.getName());

    public static final String ARCHIVE_METADATA_INDEX_DIR_MARKER = "archive-metadata.lucene.marker";

    private final ArtifactFilter filter;
    private final File indexDir;
    private final IndexWriter indexWriter;
    private FSDirectory luceneOutputDirResource;

    public static final String SHA1 = "sha1";
    public static final String GROUP_ID = "groupId";
    public static final String ARTIFACT_ID = "artifactId";
    public static final String PACKAGING = "packaging";
    public static final String CLASSIFIER = "classifier";
    public static final String VERSION = "version";


    public LuceneIndexArtifactVisitor(File outputDir, ArtifactFilter filter)
    {
        try
        {
            this.filter = filter;
            this.indexDir = outputDir;
            this.indexDir.mkdirs();
            File markerFile = new File(indexDir, getLuceneIndexDirMarkerFileName());
            FileUtils.write(markerFile, "This file is searched by Windup to locate the Lucene index with repository metadata.");

            // Create our local result index.
            this.luceneOutputDirResource = FSDirectory.open(Paths.get(indexDir.getPath()));
            StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(standardAnalyzer);
            this.indexWriter = new IndexWriter(luceneOutputDirResource, config);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed creating Lucene index writer in: " + outputDir + "\n    " + ex.getMessage(), ex);
        }
    }


    public static String getLuceneIndexDirMarkerFileName()
    {
        return ARCHIVE_METADATA_INDEX_DIR_MARKER;
    }


    @Override
    public void visit(ArtifactInfo artifact)
    {
        if (!filter.accept(artifact))
            return;
        try
        {
            // Add to Lucene index
            indexWriter.addDocuments(artifactToDocs(artifact));
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed writing to IndexWriter: " + ex.getMessage(), ex);
        }
    }


    protected Iterable<Document> artifactToDocs(ArtifactInfo artifact)
    {
        Document outputDoc = new Document();
        outputDoc.add(new StringField(SHA1, artifact.getSha1(), Field.Store.YES));
        outputDoc.add(new StringField(GROUP_ID, artifact.getGroupId(), Field.Store.YES));
        outputDoc.add(new StringField(ARTIFACT_ID, artifact.getArtifactId(), Field.Store.YES));
        outputDoc.add(new StringField(PACKAGING, artifact.getPackaging(), Field.Store.YES));
        outputDoc.add(new StringField(CLASSIFIER, artifact.getClassifier(), Field.Store.YES));
        outputDoc.add(new StringField(VERSION, artifact.getVersion(), Field.Store.YES));
        return Collections.singleton(outputDoc);
    }


    public Object done()
    {
        try
        {
            this.indexWriter.close();
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed closing Lucene index writer in: " + indexDir + "\n    " + ex.getMessage(), ex);
        }
        finally
        {
            try {
                this.luceneOutputDirResource.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

}
