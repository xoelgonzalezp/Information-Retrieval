import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TrainingTestNPL {

    public static void main(String[] args) throws Exception {

        String usage = "";
        String pathname = "index";
        StringBuilder jm = null;
        StringBuilder dir = null;
        String metric = null;
        String[] rangos;
        boolean usedjm = false;
        boolean useddir = false;
        int num1 = 0;
        int num2 = 0;
        int num3 = 0;
        int num4 = 0;
        int cut = 0;
        float valormetrica = 0;
        double bestLambda = 0.0;
        float bestMetricValue = 0;
        double bestmu = 0;
        float totalmetricatraining = 0.0F;
        float totalmetricatest = 0.0F;
        float promedio = 0;
        Object[][] matrix;

        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {
                case "-evaljm":

                    usedjm = true;
                    jm = new StringBuilder();
                    while (i + 1 < args.length && !args[i + 1].startsWith("-")) {

                        jm.append(" ").append(args[++i]);

                    }
                    jm = new StringBuilder(jm.toString().trim());
                    break;

                case "-evaldir":

                    useddir = true;
                    dir = new StringBuilder();
                    while (i + 1 < args.length && !args[i + 1].startsWith("-")) {

                        dir.append(" ").append(args[++i]);
                    }
                    dir = new StringBuilder(dir.toString().trim());
                    break;

                case "-cut":

                    cut = Integer.parseInt(args[++i]);
                    break;

                case "-metrica":

                    metric = args[++i];
                    break;

                case "-indexin":
                    pathname = args[++i];
                    break;

                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (usedjm && useddir) {
            throw new IllegalArgumentException("Either evaljm or evaldir must be set");
        } else if (usedjm || useddir) {
            rangos = (usedjm) ? jm.toString().split(" ") : dir.toString().split(" ");

            if (rangos.length == 2) {
                num1 = Integer.parseInt(rangos[0].split("-")[0]);
                num2 = Integer.parseInt(rangos[0].split("-")[1]);
                num3 = Integer.parseInt(rangos[1].split("-")[0]);
                num4 = Integer.parseInt(rangos[1].split("-")[1]);
            }
        }
        
        new File("output").mkdirs();

        assert metric != null;
        if (!metric.equals("P") && !metric.equals("R") && !metric.equals("MRR") && !metric.equals("MAP")) {

            throw new IllegalArgumentException("Unknow parameter for metrica : " + metric);
        }

        if (pathname == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try {

            System.out.println("Searching in directory '" + pathname + "'...");
            Directory directory = FSDirectory.open(Paths.get(pathname));
            try {
                IndexReader indexReader = DirectoryReader.open(directory);
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                QueryParser parser = new QueryParser("Contents", new StandardAnalyzer());
                Query[] trainingqueries = parseQuery(parser, num1, num2);
                Query[] testqueries = parseQuery(parser, num3, num4);

                if (usedjm) {

                    List<Double> lambdas = Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0);
                    List<String> header = new ArrayList<>();
                    header.add(metric + "@" + cut);

                    for (double lambda : lambdas) {
                        header.add(String.valueOf(lambda));
                    }

                    matrix = new Object[trainingqueries.length + 2][header.size()];

                    for (int j = 0; j < header.size(); j++) {
                        matrix[0][j] = header.get(j);
                    }

                    for (int i = num1; i <= num2; i++) {
                        matrix[i - num1 + 1][0] = i;
                    }

                    int aux = num1;
                    System.out.println("Training para el rango "+num1+"-"+num2+":");
                    System.out.println();
                    List<List<ScoreDoc>> trainingrelevants;
                    for (int j = 1; j < header.size(); j++) {
                        double lambda = Double.parseDouble(header.get(j));
                        int lambdaIndex = header.indexOf(String.valueOf(lambda));
                        num1 = aux;
                        trainingrelevants = parseRelevants(indexSearcher, trainingqueries, cut, (float) lambda, true, 0, num1);
                        int relvq = 0;

                        System.out.println("Lambda : "+lambda);
                        System.out.println("---------------------------");
                        System.out.println();
                        for (int i = 0; i < trainingqueries.length; i++) {
                            if (trainingqueries[i] != null) {

                                valormetrica = calcularMetrica(metric, trainingrelevants.get(i), trainingqueries[i], indexSearcher, (float) lambda, 0, cut, true, num1);
                                if (valormetrica != 0) {
                                    relvq++;
                                }
                                if(num1 < num2) {
                                    num1++;
                                }
                            }
                            assert trainingqueries[i] != null;
                            System.out.println("Query: '" + trainingqueries[i].toString().replace("Contents:", "") + "' para la métrica " + metric + "@" + cut + " tiene el valor " + valormetrica);
                            matrix[i + 1][lambdaIndex] = valormetrica;
                            totalmetricatraining += valormetrica;
                        }
                        promedio = totalmetricatraining / relvq;
                        System.out.println();
                        System.out.println("El promedio de la métrica " + metric + " es " + promedio);
                        System.out.println();
                        if (promedio > bestMetricValue) {
                            bestMetricValue = promedio;
                            bestLambda = lambda;
                        }
                        totalmetricatraining = 0;
                    }

                    System.out.println("---------------------------");
                    System.out.println();
                    System.out.println("El mejor valor de lambda es " + bestLambda + " con un resultado en la metrica " + metric + " de " + bestMetricValue);
                    num1 = aux;
                    generarCSV(matrix, "output/npl.jm.training." + num1 + "-" + num2 + ".test." + num3 + "-" + num4 + "." + metric + cut + ".training.csv", header, true);
                    List<String> headertest = new ArrayList<>();
                    headertest.add("Lambda " + bestLambda); // cabecera de la matriz con métrica y corte
                    headertest.add(metric);
                    matrix = new Object[testqueries.length + 2][headertest.size()]; // aumentar el tamaño de la matriz

                    for (int j = 0; j < headertest.size(); j++) {
                        matrix[0][j] = headertest.get(j);
                    }

                    for (int i = num3; i <= num4; i++) {
                        matrix[i - num3 + 1][0] = i;
                    }

                    List<List<ScoreDoc>> testrelevants = parseRelevants(indexSearcher, testqueries, cut, (float) bestLambda, true, 0, num3);
                    int relvq = 0;
                    System.out.println();
                    System.out.println("Test para el rango "+num3+"-"+num4+":");
                    System.out.println();
                    System.out.println("Lambda : "+bestLambda);
                    System.out.println("---------------------------");
                    System.out.println();

                    aux = num3;
                    for (int i = 0; i < testqueries.length; i++) {
                        if (testqueries[i] != null) {

                            valormetrica = calcularMetrica(metric, testrelevants.get(i), testqueries[i], indexSearcher, (float) bestLambda, 0, cut, true, num3);
                            totalmetricatest += valormetrica;

                            if (valormetrica != 0) {
                                relvq++;
                            }
                            if (num3 < num4) {
                                num3++;
                            }

                        }
                        assert testqueries[i] != null;
                        System.out.println("Query: '" + testqueries[i].toString().replace("Contents:", "") + "' para la métrica " + metric + "@" + cut + " tiene el valor " + valormetrica);
                        matrix[i + 1][1] = valormetrica;
                    }

                    promedio = totalmetricatest / relvq;
                    System.out.println("El promedio de la métrica " + metric + " es " + promedio);
                    num3 = aux;
                    generarCSV(matrix, "output/npl.jm.training." + num1 + "-" + num2 + ".test." + num3 + "-" + num4 + "." + metric + cut + ".test.csv", headertest, false);

                } else {

                    List<Integer> mus = Arrays.asList(0, 200, 400, 800, 1000, 1500, 2000, 2500, 3000, 4000);
                    List<String> header = new ArrayList<>();
                    header.add(metric + "@" + cut);

                    for (double mu : mus) {
                        header.add(String.valueOf(mu));
                    }
                    matrix = new Object[trainingqueries.length + 2][header.size()];
                    for (int j = 0; j < header.size(); j++) {
                        matrix[0][j] = header.get(j);
                    }

                    for (int i = num1; i <= num2; i++) {
                        matrix[i - num1 + 1][0] = i;
                    }

                    int aux = num1;
                    System.out.println("Training para el rango "+num1+"-"+num2+":");
                    System.out.println();
                    List<List<ScoreDoc>> trainingrelevants;

                    for (int j = 1; j < header.size(); j++) {

                        double mu = Double.parseDouble(header.get(j));
                        int muIndex = header.indexOf(String.valueOf(mu));
                        num1 = aux;
                        trainingrelevants = parseRelevants(indexSearcher, trainingqueries, cut, 0.0F, false, (int) mu, num1);
                        int relvq = 0;
                        System.out.println("Mu : "+mu);
                        System.out.println("---------------------------");
                        System.out.println();
                        for (int k = 0; k < trainingqueries.length; k++) {

                            if (trainingqueries[k] != null) {

                                valormetrica = calcularMetrica(metric, trainingrelevants.get(k), trainingqueries[k], indexSearcher, 0.1F, (int) mu, cut, false, num1);

                                if (valormetrica != 0) {
                                    relvq++;
                                }
                                if (num1 < num2) {
                                    num1++;
                                }
                            }
                            assert trainingqueries[k] != null;
                            System.out.println("Query: '" + trainingqueries[k].toString().replace("Contents:", "") + "' para la métrica " + metric + "@" + cut + " tiene el valor " + valormetrica);
                            matrix[k + 1][muIndex] = valormetrica;
                            totalmetricatraining += valormetrica;
                        }

                        promedio = totalmetricatraining / relvq;
                        System.out.println();
                        System.out.println("El promedio de la métrica " + metric + " es " + promedio);
                        System.out.println();

                        if (promedio > bestMetricValue) {
                            bestMetricValue = promedio;
                            bestmu = mu;
                        }
                        totalmetricatraining = 0;

                    }
                    System.out.println("---------------------------");
                    System.out.println();
                    System.out.println("El mejor valor de mu es " + bestmu + " con un resultado en la metrica " + metric + " de " + bestMetricValue);
                    num1 = aux;
                    generarCSV(matrix, "output/npl.dir.training." + num1 + "-" + num2 + ".test." + num3 + "-" + num4 + "." + metric + cut + ".training.csv", header, true);
                    List<String> headertest = new ArrayList<>();
                    headertest.add("Mu " + bestmu); // cabecera de la matriz con métrica y corte
                    headertest.add(metric);
                    matrix = new Object[testqueries.length + 2][headertest.size()]; // aumentar el tamaño de la matriz

                    for (int j = 0; j < headertest.size(); j++) {
                        matrix[0][j] = headertest.get(j);
                    }
                    for (int i = num3; i <= num4; i++) {
                        matrix[i - num3 + 1][0] = i;
                    }

                    List<List<ScoreDoc>> testrelevants = parseRelevants(indexSearcher, testqueries, cut, 0.0F, false, (int) bestmu, num3);
                    int relvq = 0;
                    System.out.println();
                    System.out.println("Test para el rango "+num3+"-"+num4+":");
                    System.out.println();
                    System.out.println("Mu : "+bestmu);
                    System.out.println("---------------------------");
                    System.out.println();

                    aux = num3;
                    for (int i = 0; i < testqueries.length; i++) {
                        if (testqueries[i] != null) {

                            valormetrica = calcularMetrica(metric, testrelevants.get(i), testqueries[i], indexSearcher, 0.0F, (int) bestmu, cut, false, num3);
                            totalmetricatest += valormetrica;
                            if (valormetrica != 0) {
                                relvq++;
                            }
                            if (num3 < num4) {
                                num3++;
                            }
                        }
                        assert testqueries[i] != null;
                        System.out.println("Query: '" + testqueries[i].toString().replace("Contents:", "") + "' y la métrica " + metric + "@" + cut + " tiene el valor " + valormetrica);
                        matrix[i + 1][1] = valormetrica;
                    }
                    promedio = totalmetricatest / relvq;
                    System.out.println("El promedio de la métrica " + metric + " es " + promedio);
                    num3 = aux;
                    generarCSV(matrix, "output/npl.dir.training." + num1 + "-" + num2 + ".test." + num3 + "-" + num4 + "." + metric + cut + ".test.csv", header, false);
                }
                indexSearcher.getIndexReader().close();
            } finally {
                IOUtils.close();
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }

    private static ScoreDoc[] gettotalRecuperados(IndexSearcher indexSearcher, Query query, double lambda, int cut, boolean usedjm, int mu) throws IOException { //devuelve los docs del cut para  una query
        ScoreDoc[] hits = new ScoreDoc[0];
        if (query != null) {
            if (usedjm) {
                indexSearcher.setSimilarity(new LMJelinekMercerSimilarity((float) lambda));
            } else {
                indexSearcher.setSimilarity(new LMDirichletSimilarity(mu));
            }
            TopDocs results = indexSearcher.search(query, cut);
            hits = results.scoreDocs;
        }
        return hits;
    }
    private static Query[] parseQuery(QueryParser parser, int num1, int num2) throws IOException, ParseException { //devolvemos el conjunto de queries parseadas

        List<String> result = new ArrayList<>();
        InputStream stream = Files.newInputStream(Paths.get("files/query-text"));
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            int i = 1;
            while (((line = reader.readLine()) != null)) {
                if ((i - 2) % 3 == 0) {
                    result.add(line);
                }
                i++;
            }
        }
        int queriesLength = num2 - num1 + 1;
        Query[] queries = new Query[queriesLength];
        for (int i = 0; i < queriesLength; i++) {
            String query = result.get(num1 - 1 + i);
            queries[i] = parser.parse(query.toLowerCase(Locale.ROOT));
        }
        return queries;
    }

    private static List<List<ScoreDoc>> parseRelevants(IndexSearcher indexSearcher, Query[] queries, int cut, float lambda, boolean usedjm, int mu, int num1) throws IOException { //devuelve la lista de relevantes para query del conjunto de queries
        InputStream stream2 = Files.newInputStream(Paths.get("files/rlv-ass"));
        BufferedReader reader2 = new BufferedReader(new InputStreamReader(stream2, StandardCharsets.UTF_8));
        String line;
        List<List<Integer>> relevantesIDporQuery = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean fueBarra = false;
        while ((line = reader2.readLine()) != null) {
            if (fueBarra) {
                sb.setLength(0);
                fueBarra = false;
            } else if (line.trim().equals("/")) {
                Scanner scanner = new Scanner(sb.toString());
                List<Integer> list = new ArrayList<>();
                while (scanner.hasNextInt()) {
                    list.add(scanner.nextInt());
                }
                relevantesIDporQuery.add(list);
                fueBarra = true;
            } else {
                sb.append(line).append("\n");
            }
        }

        List<List<ScoreDoc>> listaRelevantes = new ArrayList<>();
        int j = num1 - 1;
        for (int i = 0; i < queries.length; i++) {
            if (queries[i] != null) {
                if (usedjm) {
                    indexSearcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
                } else {
                    indexSearcher.setSimilarity(new LMDirichletSimilarity(mu));
                }
                TopDocs results = indexSearcher.search(queries[i], cut);
                ScoreDoc[] hits = results.scoreDocs;
                List<ScoreDoc> relevantesRecuperados = new ArrayList<>();
                for (ScoreDoc element : hits) {
                    if (relevantesIDporQuery.get(j).contains(element.doc + 1)) {
                        relevantesRecuperados.add(element);
                    }
                }
                listaRelevantes.add(relevantesRecuperados);
            }
            j++;
        }
        return listaRelevantes;
    }
    private static List<List<Integer>> relevantesporQuery() throws IOException {

        InputStream stream2 = Files.newInputStream(Paths.get("files/rlv-ass"));
        BufferedReader reader2 = new BufferedReader(new InputStreamReader(stream2, StandardCharsets.UTF_8));
        String line;
        List<List<Integer>> relevantesIDporQuery = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean fueBarra = true;
        while ((line = reader2.readLine()) != null) {
            if (fueBarra) {
                sb.setLength(0);
                fueBarra = false;
            } else if (line.trim().equals("/")) {
                Scanner scanner = new Scanner(sb.toString());
                List<Integer> list = new ArrayList<>();
                while (scanner.hasNextInt()) {
                    list.add(scanner.nextInt());
                }
                relevantesIDporQuery.add(list);
                fueBarra = true;
            } else {
                sb.append(line).append("\n");
            }
        }
        return relevantesIDporQuery;
    }
    private static float calcularMetrica(String metrica, List<ScoreDoc> relevants, Query queries, IndexSearcher indexSearcher, float lambda, int mu, int cut, boolean usedjm, int num) throws IOException {

        float valorMetrica = 0;
        List<List<Integer>> relevantesIDporQuery = relevantesporQuery();

        switch (metrica) {
            case "P":
                valorMetrica += relevants.size() / (float) cut;
                break;
            case "R": {
                valorMetrica += relevants.size() / (float) relevantesIDporQuery.get(num - 1).size();
                break;
            }
            case "MRR": {

                ScoreDoc[] hits = gettotalRecuperados(indexSearcher, queries, lambda, cut, usedjm, mu);
                float mrr = 0;
                for (int k = 1; k <= hits.length; k++) {

                    if (relevantesIDporQuery.get(num - 1).contains(hits[k - 1].doc + 1)) {

                        if (mrr == 0) {
                            mrr = 1 / (float) k;
                        }
                    }
                }
                if (relevants.size() > 0) {
                    valorMetrica += mrr;
                }
                break;
            }
            case "MAP": {

                ScoreDoc[] hits = gettotalRecuperados(indexSearcher, queries, lambda, cut, usedjm, mu);
                float apn = 0;
                int relen = 0;
                for (int k = 1; k <= hits.length; k++) {
                    if (relevantesIDporQuery.get(num - 1).contains(hits[k - 1].doc + 1)) {

                        relen++;
                        apn += relen / (float) k;
                    }
                }
                if (relevants.size() > 0) {
                    apn = apn / relevants.size();
                    valorMetrica += apn;
                }

            }
        }
        return valorMetrica;
    }

    private static void generarCSV(Object[][] matrix, String name, List<String> header, boolean training) {

        try {
            FileWriter csvWriter = new FileWriter(name);
            if (training) {
                // calculamos los promedios de cada columna
                for (int j = 1; j < header.size(); j++) {
                    double lambda = Double.parseDouble(header.get(j));
                    int lambdaIndex = header.indexOf(String.valueOf(lambda));
                    double sum = 0;
                    int count = 0;
                    for (int i = 1; i < matrix.length - 1; i++) {
                        Object value = matrix[i][lambdaIndex];
                        if (value != null && !value.toString().isEmpty() && Double.parseDouble(value.toString()) != 0.00) {
                            sum += Double.parseDouble(value.toString());
                            count++;
                        }
                    }
                    double avg = count > 0 ? sum / count : 0;
                    matrix[matrix.length - 1][lambdaIndex] = avg;
                }

            } else { //como es test solo calculamos en la columna de la métrica
                double sum = 0;
                int count = 0;
                for (int i = 1; i < matrix.length - 1; i++) {
                    Object value = matrix[i][1];
                    if (value != null && !value.toString().isEmpty() && Double.parseDouble(value.toString()) != 0.00) {
                        sum += Double.parseDouble(value.toString());
                        count++;
                    }
                }
                double avg = count > 0 ? sum / count : 0;
                matrix[matrix.length - 1][1] = avg;
            }
            matrix[matrix.length - 1][0] = "Promedio";
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    String cellValue = i == 0 || j == 0 ? String.valueOf(matrix[i][j]) : String.format("%.4f", matrix[i][j]).replace(",", ".");
                    csvWriter.append(cellValue);
                    if (j < matrix[i].length - 1) {
                        csvWriter.append(",");
                    }
                }
                if (i == 0 && matrix[i].length > 1) {
                    csvWriter.append(",");
                }
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            System.out.println("Error al guardar la matriz");
            e.printStackTrace();
        }
    }
}
