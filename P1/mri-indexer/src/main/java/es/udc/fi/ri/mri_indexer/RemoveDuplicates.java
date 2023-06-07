package es.udc.fi.ri.mri_indexer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;


import java.nio.file.Paths;

import java.util.*;

public class RemoveDuplicates {


    public static final String CONTENT = "contents";

    public static void main(String[] args) {

        String usage =
                " [-index INDEX_PATH] [-out path]\n"
                        + "This creates a new index for the specified path indicated in the argument out, removing the duplicates from the index indicated in INDEX_PATH .\n";

        String path = null;
        String out = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    path = args[++i];
                    break;
                case "-out":
                    out = args[++i];
                    break;


                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }

        if (path == null) {


            System.err.println("Usage: " + usage);
            System.exit(1);
        }


        try {


            assert path != null;
            assert out != null;
            Directory dir = FSDirectory.open(Paths.get(path));
            IndexReader reader = DirectoryReader.open(dir);


            Directory dir2 = FSDirectory.open(Paths.get(out));

            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            IndexWriter writer = new IndexWriter(dir2, config);


            Map<Integer, String> duplicados = new HashMap<>();

            System.out.println("La ruta del índice original es " + Paths.get(path) + " : Número de documentos -> " + reader.numDocs() + " , Número de términos -> " + reader.getSumTotalTermFreq(CONTENT));


            for (int i = 0; i < reader.numDocs(); i++) {

                Terms terms = reader.getTermVector(i, CONTENT);

                StringBuilder sb = new StringBuilder();

                //Aunque no haya sido almacenado, podemos acceder al vector de terminos del índice original
                if (terms != null) {
                    TermsEnum termsEnum = terms.iterator();
                    BytesRef term;
                    while ((term = termsEnum.next()) != null) {
                        String termText = term.utf8ToString();
                        sb.append(termText).append("\n");
                    }
                }

                Document document = new Document();
                document.add(new StringField("path", reader.document(i).get("path"), Field.Store.YES));
                document.add(new TextField("contents", sb.toString(), Field.Store.YES));
                System.out.println("Añadiendo al índice el documento " + Paths.get(document.get("path")).getFileName().toFile());
                writer.addDocument(document);


            }


            for (int i = 0; i < reader.numDocs(); i++) {
                for (int j = 0; j < reader.numDocs(); j++) {

                    if (reader.document(i).get(CONTENT) != null && reader.document(j).get(CONTENT) != null) { //si el campo ha sido almacenado


                        if (i == j) {
                            continue;
                        }

                        if (duplicados.containsKey(j)) {
                            continue;
                        }


                        if (reader.document(i).get(CONTENT).equals(reader.document(j).get(CONTENT))) { //si el contenido de los campos es idéntico

                            String filename = reader.document(i).get("path");
                            String duplicateFilename = reader.document(j).get("path");

                            if (!duplicados.containsValue(duplicateFilename)) {
                                duplicados.put(j, filename);

                            }

                            if (duplicados.containsValue(filename)) {
                                break;
                            }

                        }
                    } else { //si no ha sido almacenado, podemos mirar si el hash es el mismo


                        if (i == j) {
                            continue;
                        }

                        if (duplicados.containsKey(j)) {
                            continue;
                        }


                        String hash1 = reader.document(i).get("hash");

                        String hash2 = reader.document(j).get("hash");

                        if (hash1.equals(hash2)) {

                            String filename = reader.document(i).get("path");
                            String duplicateFilename = reader.document(j).get("path");

                            if (!duplicados.containsValue(duplicateFilename)) {
                                duplicados.put(j, filename);

                            }

                            if (duplicados.containsValue(filename)) {
                                break;
                            }

                        }

                    }

                }

            }


            for (Map.Entry<Integer, String> entry : duplicados.entrySet()) { //elimina el duplicado del indice

                System.out.println("Eliminando el documento " + entry.getValue());
                writer.deleteDocuments(new Term("path", entry.getValue()));


            }


            reader.close();
            writer.forceMergeDeletes(); //así aseguramos los cambios en el índice
            writer.commit();
            writer.close();

            IndexReader reader2 = DirectoryReader.open(dir2);


            System.out.println("La ruta del nuevo índice es " + Paths.get(out) + " : Número de documentos -> " + reader2.numDocs() + " , Número de términos -> " + reader2.getSumTotalTermFreq(CONTENT));

            reader2.close();


        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }

    }

}