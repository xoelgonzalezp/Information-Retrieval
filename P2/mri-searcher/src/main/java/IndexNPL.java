import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;


public class IndexNPL implements AutoCloseable {

    public static void main(String[] args) throws Exception {

        String usage = "";

        String openmode = "";
        String pathname = "index";
        String pathaname = null;
        String indexingmodel = "";
        Analyzer analyzer = null;
        IndexWriterConfig indexWriterConfig;
        IndexWriterConfig.OpenMode openMode = null;
        boolean jm = false;
        float value = 0;

        Path stopwordsfile = Paths.get("files/stopwords.txt");
        List<String> stopwordsList = Files.readAllLines(stopwordsfile);
        CharArraySet stopwords = new CharArraySet(stopwordsList, true);

        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {

                case "-openmode":

                    openmode = args[++i];

                    switch (openmode) {

                        case "create":

                            openMode = IndexWriterConfig.OpenMode.CREATE;
                            break;
                        case "append":

                            openMode = IndexWriterConfig.OpenMode.APPEND;
                            break;
                        case "create_or_append":

                            openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
                            break;
                        default:

                            throw new IllegalArgumentException("unknown openmode " + openmode);
                    }
                    break;

                case "-index":
                    pathname = args[++i];
                    break;
                case "-docs":
                    pathaname = args[++i];
                    break;

                case "-analyzer":

                    String analyzer1 = args[++i];

                    switch (analyzer1) {

                        case "StandardAnalyzer":

                            analyzer = new StandardAnalyzer();
                            break;

                        case "WhitespaceAnalyzer":

                            analyzer = new WhitespaceAnalyzer();
                            break;

                        case "SimpleAnalyzer":

                            analyzer = new SimpleAnalyzer();
                            break;

                        case "StopAnalyzer":

                            analyzer = new StopAnalyzer(stopwords);
                            break;

                        case "KeywordAnalyzer":

                            analyzer = new KeywordAnalyzer();
                            break;

                        case "EnglishAnalyzer":

                            analyzer = new EnglishAnalyzer();
                            break;

                        case "FrenchAnalyzer":

                            analyzer = new FrenchAnalyzer();
                            break;

                        case "SpanishAnalyzer":

                            analyzer = new SpanishAnalyzer();
                            break;

                        case "GermanAnalyzer":

                            analyzer = new GermanAnalyzer();
                            break;

                        case "ItalianAnalyzer":

                            analyzer = new ItalianAnalyzer();
                            break;

                        default:
                            throw new IllegalArgumentException("unknown analyzer" + indexingmodel);

                    }

                    break;

                case "-indexingmodel":

                    indexingmodel = args[++i];

                    switch (indexingmodel) {

                        case "jm":

                            jm = true;
                            value = Float.parseFloat(args[++i]);
                            break;

                        case "dir":

                            jm = false;
                            value = Float.parseFloat(args[++i]);
                            break;

                        default:
                            throw new IllegalArgumentException("unknown indexing model " + indexingmodel);
                    }
                    break;

                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }
        
        new File("output").mkdirs();

        if (pathaname == null || pathname == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (analyzer == null) {

            analyzer = new StandardAnalyzer();
        }

        if (openMode == null) {

            openMode = IndexWriterConfig.OpenMode.CREATE;
        }

        indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriterConfig.setOpenMode(openMode);

        if (jm) {

            indexWriterConfig.setSimilarity(new LMJelinekMercerSimilarity(Float.parseFloat(String.valueOf(value))));

        } else {
            indexWriterConfig.setSimilarity(new LMDirichletSimilarity(Float.parseFloat(String.valueOf(value))));

        }

        final Path docDir = Paths.get(pathaname);

        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath() + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        try {

            System.out.println("Indexing to directory '" + pathname + "'...");
            Directory dir = FSDirectory.open(Paths.get(pathname));

            try (IndexWriter writer = new IndexWriter(dir, indexWriterConfig); FileInputStream stream = new FileInputStream(docDir.toFile()); IndexNPL indexNPL = new IndexNPL()) { //se supone un solo archivo

                System.out.println("Indexing the file named '" + docDir.toFile().getName() + "'...");
                indexNPL.indexDoc(writer, docDir);
                writer.commit();

            } finally {
                IOUtils.close();
            }

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }

    }

    void indexDoc(IndexWriter writer, Path file) throws IOException {

        try (InputStream stream = Files.newInputStream(file)) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            String line;
            Document doc = null;
            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                if (Character.isDigit(line.charAt(0))) {

                    if (doc != null) {
                        doc.add(new TextField("Contents", sb.toString(), Field.Store.YES));
                        writer.addDocument(doc);
                    }

                    doc = new Document();
                    doc.add(new StringField("DocIDNPL", line.trim(), Field.Store.YES));
                    sb.setLength(0);

                } else if (line.trim().equals("/")) {

                    if (doc != null) {
                        doc.add(new TextField("Contents", sb.toString(), Field.Store.YES));
                        writer.addDocument(doc);
                        doc = null;
                        sb.setLength(0);
                    }

                } else {
                    sb.append(line).append("\n");
                }
            }

            if (doc != null) {
                doc.add(new TextField("Contents", sb.toString(), Field.Store.YES));
                writer.addDocument(doc);
            }

        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }

}
