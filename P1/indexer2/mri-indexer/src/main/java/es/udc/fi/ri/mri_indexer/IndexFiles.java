package es.udc.fi.ri.mri_indexer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexFiles implements AutoCloseable {

    //La variable vectorDict es un diccionario de vectores
    // demoEmbeddings es una instancia se utiliza para calcular vectores de incrustación utilizando el diccionario de vectores vectorDict
    static final String KNN_DICT = "knn-dict"; //se utiliza para identificar un índice de vectores de incrustación de búsqueda KNN (KnnVector)

    // Calcula los vectores de incrustación para la búsqueda KnnVector
    private final DemoEmbeddings demoEmbeddings; //se usa para calcular vectores de incrustación utilizando vectorDict
    private final KnnVectorDict vectorDict; //diccionario de vectores

    private IndexFiles(KnnVectorDict vectorDict) throws IOException {
        if (vectorDict != null) {
            this.vectorDict = vectorDict;
            demoEmbeddings = new DemoEmbeddings(vectorDict);
        } else {
            this.vectorDict = null;
            demoEmbeddings = null;
        }
    }


    public static void main(String[] args) throws Exception {
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles\n"
                        + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexPath = "index";
        String docsPath = null;
        String vectorDictSource = null;
        boolean create = true;
        boolean contentsStored = false;
        boolean contentsTermVectors = false;
        int numThreads = Runtime.getRuntime().availableProcessors();
        int depth = -1;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
                    break;
                case "-update":
                    create = false;
                    break;
                case "-create":
                    create = true;
                    break;

                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;

                case "-contentsStored":
                    contentsStored = true;
                    break;

                case "-contentsTermVectors":
                    contentsTermVectors = true;
                    break;

                case "-depth":
                    depth = Integer.parseInt(args[++i]);
                    break;


                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }

        if (docsPath == null) { //si no hay ruta para el doc nos manda el uso
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (depth < -1) {
            throw new IllegalArgumentException("La profundidad tiene que tomar un valor positivo");
        }


        final Path docDir = Paths.get(docsPath); //guarda en docdir la ruta del doc

        if (!Files.isReadable(docDir)) { // si es incorrecta o no se puede leer la ruta manda el mensaje
            System.out.println("Document directory '" + docDir.toAbsolutePath() + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }


        Date start = new Date(); //crea el objeto date que representa el momento de creacion del indice


        try {
            System.out.println("Indexing to directory '" + indexPath + "'..."); //indica el directorio en el que se va a almacenar el índice

            Directory dir = FSDirectory.open(Paths.get(indexPath)); //dir representa el directorio en el que se va a almacenar el indice indicado por indexpath
            Analyzer analyzer = new StandardAnalyzer(); // Analyzer que se utilizará para analizar los documentos que se van a indexar


            IndexWriterConfig iwc = new IndexWriterConfig(analyzer); // Crea un objeto IndexWriterConfig que se utiliza para configurar la creación del índice.


            if (create) {
                // hace un nuevo índice en el directorio, eliminando los docs previamente indexados
                iwc.setOpenMode(OpenMode.CREATE);
            } else {

                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Opcional
            // iwc.setRAMBufferSizeMB(256.0);

            //hace un diccionario de vectores
            KnnVectorDict vectorDictInstance = null;
            long vectorDictSize = 0;
            if (vectorDictSource != null) {
                KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
                vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
                vectorDictSize = vectorDictInstance.ramBytesUsed();
            }


            final ExecutorService executor = Executors.newFixedThreadPool(numThreads); //hacemos el pool de threads
            List<IndexWriter> Listapartialwriter = new ArrayList<>(); //lista para guardar los índices parciales

            try (IndexWriter writer = new IndexWriter(dir, iwc); DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir); IndexFiles indexFiles = new IndexFiles(vectorDictInstance)) {


                for (final Path path : directoryStream) {

                    if (Files.isDirectory(path)) { //si el path es una carpeta, crea su índice parcial


                        String sufijo = "carpeta_parcial_" + path.toFile().getName();
                        Path partialPath = Paths.get(indexPath).getParent().resolve(sufijo);
                        IndexWriterConfig partialindexwriteriwc = new IndexWriterConfig(new StandardAnalyzer());

                        if (create) {

                            partialindexwriteriwc.setOpenMode(OpenMode.CREATE);
                        } else {

                            partialindexwriteriwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                        }


                        Directory partialDir = FSDirectory.open(partialPath);
                        IndexWriter partialwriter = new IndexWriter(partialDir, partialindexwriteriwc);
                        Listapartialwriter.add(partialwriter);
                        executor.execute(new WorkerThread(path, partialwriter, contentsStored, contentsTermVectors, depth, indexFiles));
                    }

                }
                // Esperamos a que se completen todos los hilos antes de cerrar los IndexWriter
                executor.shutdown();


                try {
                    executor.awaitTermination(1, TimeUnit.HOURS);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                    System.exit(-2);
                }

                for (IndexWriter partialwriter : Listapartialwriter) { //fusionamos todos los índices parciales en el índice global

                    partialwriter.commit(); //hace el commit de todos los cambios
                    partialwriter.close();
                    writer.addIndexes(partialwriter.getDirectory());

                }
                writer.commit();


            } finally {
                IOUtils.close(vectorDictInstance);
            }


            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime()) + " milliseconds");
                if (reader.numDocs() > 100 && vectorDictSize < 1_000_000 && System.getProperty("smoketester") == null) {
                    throw new RuntimeException(
                            "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
                }
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }



    @Override
    public void close() throws IOException {
        IOUtils.close(vectorDict);
    }


    static class WorkerThread implements Runnable {
        private final IndexFiles indexFiles;
        private final Path folder;
        private final IndexWriter writer;
        private final boolean contentsStored;
        private final boolean contentsTermVectors;
        private final int depth;

        public WorkerThread(final Path folder, IndexWriter writer, boolean contentsStored, boolean contentsTermVectors, int depth, IndexFiles indexFiles) {

            this.indexFiles = indexFiles;
            this.folder = folder;
            this.writer = writer;
            this.contentsStored = contentsStored;
            this.contentsTermVectors = contentsTermVectors;
            this.depth = depth;

        }

        @Override
        public void run() {
            try {
                System.out.println(String.format("Soy el hilo %s y voy a indexar las entradas de la carpeta %s", Thread.currentThread().getName(), folder.toFile().getName()));



                // Indexamos los archivos en el subdirectorio
                indexFiles.indexDocs(writer, folder, contentsStored, contentsTermVectors, depth);

                System.out.println(String.format("Soy el hilo %s y he acabado de indexar las entradas de la carpeta %s", Thread.currentThread().getName(), folder.toFile().getName()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }


    void indexDocs(final IndexWriter writer, Path path, boolean contentsStored, boolean contentsTermVectors, int depth) throws IOException {


        Files.walkFileTree(path, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {


                if (dir.getParent().equals(path)) { // si es una carpeta de primer nivel, continuamos visitando

                    return FileVisitResult.CONTINUE;

                } else if (!Files.isDirectory(dir)) { //si además de estar en el primer nivel es un archivo, lo omitimos
                    return FileVisitResult.SKIP_SUBTREE;

                }
                return FileVisitResult.CONTINUE; //en cualquier otro caso, continuamos

            }


            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                ClassLoader classLoader = getClass().getClassLoader();
                InputStream inputStream = classLoader.getResourceAsStream("config.properties");
                Properties properties = new Properties();
                properties.load(inputStream);


                try {

                    if (depth == 0) { //cuando el límite sea 0, no indexamos nada

                        return FileVisitResult.SKIP_SUBTREE;

                    } else if (depth == 1) { //solo indexamos los archivos que cuelgan de la carpeta de primer nivel

                        if (preVisitDirectory(file, attrs).equals(FileVisitResult.CONTINUE)) { //si al previsitar las carpetas y archivos de primer nivel, si su parent es la carpeta de primer nivel, indexamos, si no, no

                            if (properties.getProperty("notFiles") == null && properties.getProperty("onlyFiles") == null || properties.getProperty("notFiles") == null) {
                                indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                return FileVisitResult.SKIP_SUBTREE;
                            }


                            if (!(properties.getProperty("onlyFiles") == null)) {

                                if (Arrays.stream(properties.getProperty("onlyFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                    return FileVisitResult.CONTINUE;
                                }
                            } else if (!(properties.getProperty("notFiles") == null)) {
                                if (Arrays.stream(properties.getProperty("notFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                                    return FileVisitResult.SKIP_SUBTREE;

                                } else {

                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                    return FileVisitResult.CONTINUE;
                                }
                            }

                        }
                        return FileVisitResult.SKIP_SUBTREE; //omitimos carpetas, ya que entraríamos en un depth mayor que 1

                    } else if (depth > 1) {

                        int current = path.relativize(file).getNameCount();

                        if (current > depth) { // si se ha alcanzado el límite del depth, omitimos la carpeta

                            return FileVisitResult.SKIP_SUBTREE;

                        } else { // si no se ha alcanzado el límite del depth, seguimos


                            if (properties.getProperty("notFiles") == null && properties.getProperty("onlyFiles") == null || properties.getProperty("notFiles") == null) {
                                indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                return FileVisitResult.SKIP_SUBTREE;
                            }


                            if (!(properties.getProperty("onlyFiles") == null)) {

                                if (Arrays.stream(properties.getProperty("onlyFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                    return FileVisitResult.CONTINUE;
                                }
                            } else if (!(properties.getProperty("notFiles") == null)) {
                                if (Arrays.stream(properties.getProperty("notFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                                    return FileVisitResult.SKIP_SUBTREE;

                                } else {

                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                                    return FileVisitResult.CONTINUE;
                                }
                            }


                        }

                    }


                    //no hay límite en la indexación

                    if (properties.getProperty("notFiles") == null && properties.getProperty("onlyFiles") == null || properties.getProperty("notFiles") == null) {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                        return FileVisitResult.SKIP_SUBTREE;
                    }


                    if (!(properties.getProperty("onlyFiles") == null)) {

                        if (Arrays.stream(properties.getProperty("onlyFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                            return FileVisitResult.CONTINUE;
                        }
                    } else if (!(properties.getProperty("notFiles") == null)) {
                        if (Arrays.stream(properties.getProperty("notFiles").split("")).anyMatch(extension -> file.getFileName().toString().endsWith(extension))) {

                            return FileVisitResult.SKIP_SUBTREE;

                        } else {

                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, depth);
                            return FileVisitResult.CONTINUE;
                        }
                    }

                } catch (
                        @SuppressWarnings("unused")
                        IOException ignore) {
                    ignore.printStackTrace(System.err);
                }

                return FileVisitResult.CONTINUE; //como no hay límite en la indexación, continuamos visitando
            }

        });
    }


    void indexDoc(IndexWriter writer, Path file, long lastModified, boolean contentsStored, boolean contentsTermVectors, int depth) throws IOException {



        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

        try (InputStream stream = Files.newInputStream(file)) {

            Document doc = new Document(); // documento que se va a indexar
            // Se agrega un campo al documento para la ruta, usando StringField para crear un campo de tipo String que se almacena en el índice pero no se tokeniza ni se indexa la frecuencia del término
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);


            doc.add(new LongPoint("modified", lastModified));


            Field contents;
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line = "";
            int current = 0;


            if (properties.getProperty("onlyLines") == null) {

                while ((line = reader.readLine()) != null) {
                    current++;
                    sb.append(line).append("\n");
                }

            } else {
                int onlyLines = Integer.parseInt(properties.getProperty("onlyLines"));
                while ((line = reader.readLine()) != null && current < onlyLines) {
                    current++;
                    sb.append(line).append("\n");
                }

            }


            final FieldType TYPE_STORED = new FieldType(); //FieldType hecho a medida
            final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

            //con la opción contentsStored indicamos que el campo contents tiene que ser almacenado
            if (contentsStored) {

                contents = new StringField("contents", sb.toString(), Field.Store.YES);
                TYPE_STORED.setStored(true); //el fieldtype también será almacenado

            } else {

                contents = new TextField("contents", sb.toString(), Field.Store.NO);
                TYPE_STORED.setStored(false); //el fieldtype no será almacenado

                doc.add(new StringField("hash", Arrays.toString(getHash(sb.toString())),Field.Store.YES)); //cuando contents no sea almacendo usamos un hash para luego identificar duplicados
            }

            //con la opción contentsTermvectors indicamos que el campo contents tiene que ser almacenado
            if (contentsTermVectors) {
                TYPE_STORED.setIndexOptions(options);
                TYPE_STORED.setTokenized(true);
                TYPE_STORED.setStoreTermVectors(true);
                TYPE_STORED.setStoreTermVectorPositions(true);
                TYPE_STORED.freeze();
                contents = new Field("contents", sb.toString(), TYPE_STORED);
            }


            doc.add(contents);
            doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
            doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));

            String type;
            if (Files.isRegularFile(file)) {

                type = "regular file";

            } else if (Files.isDirectory(file)) {

                type = "directory";

            } else if (Files.isSymbolicLink(file)) {

                type = "symbolic link";

            } else {

                type = "other";

            }

            doc.add(new StringField("type", type, Field.Store.YES));
            long size = Files.size(file) / 1000;
            doc.add(new StringField("sizeKb", Long.toString(size), Field.Store.YES));
            FileTime creationTime = (FileTime) Files.getAttribute(file, "creationTime");
            FileTime lastAccessTime = (FileTime) Files.getAttribute(file, "lastAccessTime");
            FileTime lastModifiedTime = (FileTime) Files.getAttribute(file, "lastModifiedTime");
            doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
            doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
            doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));
            doc.add(new StringField("creationTimeLucene", DateTools.dateToString(Date.from(creationTime.toInstant()), DateTools.Resolution.DAY), Field.Store.YES));
            doc.add(new StringField("lastAccessTimeLucene", DateTools.dateToString(Date.from(lastAccessTime.toInstant()), DateTools.Resolution.DAY), Field.Store.YES));
            doc.add(new StringField("lastModifiedTimeLucene", DateTools.dateToString(Date.from(lastModifiedTime.toInstant()), DateTools.Resolution.DAY), Field.Store.YES));
            //si existe demoEmbeddings, se añade un campo de tipo KnnVectorField
            if (demoEmbeddings != null) {
                try (InputStream in = Files.newInputStream(file)) {
                    float[] vector = demoEmbeddings.computeEmbedding(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
                    doc.add(new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
                }
            }

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {

                System.out.println("adding " + file);
                writer.addDocument(doc);

            } else {
                // se hace el update del archivo si el índice es existente
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }


    //usamos esta función getHash para obtener el hash del campo contents en caso de que ese field no sea almacenado(no hay la opción -contentsStored)
    static byte[] getHash(String contents) {
        
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(contents.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Error al obtener el hash: " + e.getMessage());
            return null;
        }
    }

}