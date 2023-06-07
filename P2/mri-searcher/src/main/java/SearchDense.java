import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.opencsv.CSVWriter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;


public class SearchDense {
    Similarity similarity;
    int top;
    int cut;
    float start;
    float fin;
    String indexpath;
    String name, name2;
    int knnVectors, hitsPerPage;

    public SearchDense(Similarity similarity, int top, int cut, float start, float fin, String indexpath, String name, String name2, int knnVectors, int hitsPerPage) {
        this.similarity = similarity;
        this.top = top;
        this.cut = cut;
        this.start = start;
        this.fin = fin;
        this.indexpath = indexpath;
        this.name = name;
        this.name2 = name2;
        this.knnVectors = knnVectors;
        this.hitsPerPage = hitsPerPage;
    }

    public void run() throws IOException, ParseException {
        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexpath)));
        IndexSearcher searcher = new IndexSearcher(indexReader);
        searcher.setSimilarity(similarity);
        Analyzer analyzer = new StandardAnalyzer();
        KnnVectorDict vectorDict = null;
        if (knnVectors > 0) {
            vectorDict = new KnnVectorDict(indexReader.directory(), IndexDense.KNN_DICT);
        }


        InputStream stream = Files.newInputStream(Paths.get("files/query-text"));
        InputStream stream2 = Files.newInputStream(Paths.get("files/rlv-ass"));

        File myObj = new File("output/"+name);
        myObj.createNewFile();
        FileWriter myWriter = new FileWriter("output/"+name);

        File file = new File("output/"+name2);
        FileWriter outputfile = new FileWriter(file);
        CSVWriter writer = new CSVWriter(outputfile);
        String[] header = {"QueryID", "P@"+cut,"Recall@"+cut, "RR@"+cut, "AP@"+cut};
        writer.writeNext(header);

        List<String> result = new ArrayList<>();
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, "UTF-8"));
        try{
            String line;
            int i= 1;
            while (((line = reader.readLine()) != null)) {
                if((i-2)%3==0){
                    result.add(line);
                }
                i++;
            }
        }finally{
            reader.close();
        }



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



        QueryParser parser = new QueryParser("Contents", analyzer);
        float mrr=0;
        float map = 0;
        float mpn =0;
        float mrecall = 0;
        int relvq=0;
        for(int i = 0; i<result.size() && i<fin;i++){
            if(i<(start-1)){
                continue;
            }
            Query query =parser.parse(result.get(i).toLowerCase(Locale.ROOT));
            if (knnVectors > 0) {
                query = addSemanticQuery(query, vectorDict, knnVectors);
            }
            TopDocs results = searcher.search(query, cut);
            ScoreDoc[] hits = results.scoreDocs;
            List<ScoreDoc> listaRelevantes = new ArrayList<>();
            String str = "Query: "+result.get(i).toLowerCase(Locale.ROOT)+"\n";
            myWriter.write(str+"\n");
            System.out.println(str);
            int cont = 1;
            for (ScoreDoc element : hits) {
                if(cont<=top){
                    Document doc = indexReader.document(element.doc);
                    String contents = doc.getField("Contents").stringValue();
                    str = cont+" DocIdNPL: "+doc.getField("DocIDNPL").stringValue()+" score: "+element.score;
                    myWriter.write(str);
                    System.out.println(str);
                    str = "Contents: "+contents;
                    myWriter.write(str+"\n");
                    System.out.println(str);
                }
                if (relevantesIDporQuery.get(i).contains(element.doc+1)) {
                    if(cont<=top){
                        str = "Relevante: true\n";
                        myWriter.write(str+"\n");
                        System.out.println(str);
                    }
                    listaRelevantes.add(element);
                }else{
                    if(cont<=top){
                        str = "Relevante: false\n";
                        myWriter.write(str+"\n");
                        System.out.println(str);
                    }
                }
                cont++;
            }
            int numeroRelevantesTotal = relevantesIDporQuery.get(i).size();
            int numeroRelevantesRecuuperados = listaRelevantes.size();
            int numeroRecuperados = hits.length;

            float pn = numeroRelevantesRecuuperados/(float)cut;
            float recall = numeroRelevantesRecuuperados/(float)numeroRelevantesTotal;
            float apn = 0;
            int relen=0;
            float rr=0;
            for(int k=1 ;k<=numeroRecuperados;k++){
                if(relevantesIDporQuery.get(i).contains(hits[k-1].doc+1)){
                    if(rr==0){
                        rr=1/(float)k;
                    }
                    relen++;
                    apn+=relen/(float)k;
                }
            }
            if (numeroRelevantesRecuuperados == 0){
                apn = 0;
            }else{
                apn = apn/numeroRelevantesRecuuperados;
            }
            if(pn != 0){
                relvq++;
            }
            mrr +=rr;
            map +=apn;
            mpn +=pn;
            mrecall += recall;
            str = "Métricas de la query "+(i+1)+" -> pn: "+pn+"  recall: "+recall+"  apn: "+apn+"  rr: "+rr+"\n\n\n";
            header = new String[]{"" + (i + 1),""+pn, ""+recall,""+rr,""+apn};
            writer.writeNext(header);
            myWriter.write(str+"\n");
            System.out.println(str);
            /*for(int j = 0;j<top && j< hits.length;j++){
                System.out.println("doc=" + hits[j].doc + " score=" + hits[j].score);
            }*/
        }
        mrr = mrr/relvq;
        map = map/relvq;
        mpn = mpn/relvq;
        mrecall = mrecall/relvq;
        String str = "Métricas media -> mpn: "+mpn+"  mrecall: "+mrecall+"  map: "+map+"  mrr: "+mrr+"\n";
        header = new String[]{"Medias",""+mpn, ""+mrecall,""+mrr,""+map};
        writer.writeNext(header);
        myWriter.write(str+"\n");
        System.out.println(str);
        myWriter.close();
        writer.close();

    }


    private static Query addSemanticQuery(Query query, KnnVectorDict vectorDict, int k)
            throws IOException {
        StringBuilder semanticQueryText = new StringBuilder();
        QueryFieldTermExtractor termExtractor = new QueryFieldTermExtractor("Contents");
        query.visit(termExtractor);
        for (String term : termExtractor.terms) {
            semanticQueryText.append(term).append(' ');
        }
        if (semanticQueryText.length() > 0) {
            KnnVectorQuery knnQuery =
                    new KnnVectorQuery(
                            "contents-vector",
                            new DemoEmbeddings(vectorDict).computeEmbedding(semanticQueryText.toString()),
                            k);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, BooleanClause.Occur.SHOULD);
            builder.add(knnQuery, BooleanClause.Occur.SHOULD);
            return builder.build();
        }
        return query;
    }
    private static class QueryFieldTermExtractor extends QueryVisitor {
        private final String field;
        private final List<String> terms = new ArrayList<>();

        QueryFieldTermExtractor(String field) {
            this.field = field;
        }

        @Override
        public boolean acceptField(String field) {
            return field.equals(this.field);
        }

        @Override
        public void consumeTerms(Query query, Term... terms) {
            for (Term term : terms) {
                this.terms.add(term.text());
            }
        }

        @Override
        public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            if (occur == BooleanClause.Occur.MUST_NOT) {
                return QueryVisitor.EMPTY_VISITOR;
            }
            return this;
        }
    }

}
