package es.udc.fi.ri.mri_indexer;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.nio.file.Paths;
import java.util.PriorityQueue;
import java.util.*;

public class TopTermsInDocs {
    public static final String CONTENT = "contents";

    public static void main(String[] args) throws Exception {

        String usage =
                " [-index INDEX_PATH] [-docID int1-int2] [-top n] [-outfile path]\n"
                        + "This retrieves tf,df and (tf x idflog10) for the documents from the indicated INDEX_PATH that are in the specified range indicated in docID.\n"
                        + "The argument top sorts the n of terms for each document by the highest (raw tf) x idflog10.\n"
                        + "The argument outfile stores in a file for the specified path the results showed on screen.";
        String path = null;
        String outfile = null;
        String docID = null;
        int rango1 = 0;
        int rango2 = 0;
        int tf = 0;
        int df = 0;
        double idf = 0;
        double weight = 0;
        int top = -1;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    path = args[++i];
                    break;
                case "-docID":
                    docID = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;

                case "-outfile":
                    outfile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }

        if (docID != null) {

            String[] rangos = docID.split("-");

            if (rangos.length >= 2 && rangos[0].trim().length() > 0 && rangos[1].trim().length() > 0) { //almacenamos los rangos

                rango1 = Integer.parseInt((rangos[0].trim()));
                rango2 = Integer.parseInt((rangos[1].trim()));

            }

        } else {

            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (top <= 0 && top != -1) { //si el arg de top no es válido salta excepción

            throw new IllegalArgumentException("top tiene que tomar valores mayores que 0");
        }


        assert outfile != null;
        try (PrintWriter printWriter = new PrintWriter(outfile)) { //usamos un PrintWriter para mandar al outFile lo que necesitamos
            assert path != null;
            Directory dir = FSDirectory.open(Paths.get(path));
            IndexReader reader = DirectoryReader.open(dir);


            for (reader.document(rango1); rango1 <= rango2; rango1++) { //recorremos con el reader para el rango especificado

                Map<String, Integer> frequencies = getTermFrequencies(reader, rango1);

                if (top > reader.getTermVector(rango1, CONTENT).size()) { //el top no puede ser más alto que el número de términos en el documento

                    throw new IllegalArgumentException("top no puede ser más alto que el número de terms en el documento " + Paths.get(reader.document(rango1).get("path")).getFileName().toFile());

                }

                printWriter.write("Documento " + Paths.get(reader.document(rango1).get("path")).getFileName().toFile() + " con DocID " + rango1 + "\n");
                System.out.println("Documento " + Paths.get(reader.document(rango1).get("path")).getFileName().toFile() + " con DocID " + rango1 + "\n");
                assert frequencies != null;

                if (top > 0) { //si solo se pueden enseñar el top n términos de cada doc ordenados por (raw tf) x idflog10


                    PriorityQueue<Peso> queue = new PriorityQueue<>(top, Collections.reverseOrder()); //hacemos una cola de prioridad, con reverseorder el elemento de mayor valor tiene máx prioridad

                    for (Map.Entry<String, Integer> entry : frequencies.entrySet()) { //recorremos el mapa

                        String term = entry.getKey(); //para cada entry obtenemos el term asociado en su key
                        tf = entry.getValue(); // tf, número de veces que aparece en en ese doc el term, es decir la frecuencia asociada

                        df = reader.docFreq(new Term(CONTENT, term)); //df, número de veces que aparece en todos los documentos el term
                        idf = Math.log10((double) reader.numDocs() / (double) df);
                        weight = tf * idf; //(raw tf) x idflog10
                        Peso peso = new Peso(term, tf, df, idf, weight); //hacemos un objeto peso

                        queue.offer(peso); //inserta el peso en la priority queue
                    }
                    int current = 0;
                    while (!queue.isEmpty() && current < top) { //si la queue no está vacía y aún no hemos llegado al top

                        Peso peso = queue.poll();  //quitamos de la priority queue la cabeza

                        printWriter.write("Term top " + current + "\n");
                        printWriter.write(peso.term + " ( tf : " + peso.tf + " , df : " + peso.df + " , (raw tf) x idflog10 : " + peso.weight + ")\n");

                        System.out.println("Term top " + current + "\n");
                        System.out.println(peso.term + " ( tf : " + peso.tf + " , df : " + peso.df + " , (raw tf) x idflog10 : " + peso.weight + ")\n");

                        current++;
                    }

                } else { //si no hay top -n

                    for (Map.Entry<String, Integer> entry : frequencies.entrySet()) { //en el mapa de frecuencias, para cada term obtenemos su tf,df,idf,weight

                        String term = entry.getKey();
                        tf = entry.getValue();

                        df = reader.docFreq(new Term(CONTENT, term));
                        idf = Math.log10((double) reader.numDocs() / (double) df);
                        weight = tf * idf;

                        printWriter.write(term + "( tf : " + tf + " , df : " + df + " , (raw tf) x idflog10 : " + weight + ")\n");
                        System.out.println(term + " ( tf : " + tf + " , df : " + df + " , (raw tf) x idflog10 : " + weight + ")\n");

                    }

                }
            }

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }

    }

    static Map<String, Integer> getTermFrequencies(IndexReader reader, int docID) throws IOException {

        Terms vector = reader.getTermVector(docID, CONTENT); //obtenemos el vector de términos para el campo contents y en el docId especificado

        if (vector == null) {
            System.err.println("No se encuentra el vector para el Documento " + docID);
            return null;
        }

        TermsEnum termsEnum = null;
        termsEnum = vector.iterator();
        Map<String, Integer> frequencies = new HashMap<>(); //hacemos un mapa para las frecuencias : key -> el term, value ->  la frecuencia asociada al term
        List<String> terms = new ArrayList<>();
        BytesRef text = null;
        while ((text = termsEnum.next()) != null) {
            String term = text.utf8ToString();
            int freq = (int) termsEnum.totalTermFreq();
            frequencies.put(term, freq);
            terms.add(term);
        }
        return frequencies; //devolvemos el mapa
    }

    static class Peso implements Comparable<Peso> {
        public String term;
        public double weight;
        public int tf;
        public int df;
        public double idf;

        public Peso(String term, int tf, int df, double idf, double weight) {
            this.term = term;
            this.weight = weight;
            this.tf = tf;
            this.df = df;
            this.idf = idf;
        }

        @Override
        public int compareTo(Peso peso2) {
            return Double.compare(this.weight, peso2.weight); //comparamos el weight de cada objeto
        }
    }
}
