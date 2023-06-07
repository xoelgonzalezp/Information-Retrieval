import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.File;

public class DenseRetrieval {
    public static void main(String[] args) throws Exception {
        Similarity search;
        String searchStr = "";
        float value = 0;
        String indexPath = "";
        String[] parts = new String[2];
        int cut = 0;
        int top = 0;
        int start = -1;
        int fin = 100;
        boolean acabar = false;
        int knnVectors = 0;
        int hitsPerPage = 10;
        String str = "";
        for (int i = 0; !acabar && (i < args.length); i++) {
            switch (args[i]) {
                case "-search":
                    searchStr = args[++i];
                    value = Float.parseFloat(args[++i]);
                    break;
                case "-indexin":
                    indexPath = args[++i];
                    break;
                case "-indexar":
                    IndexDense.run(args);
                    acabar = true;
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-knnVectors":
                    knnVectors = Integer.parseInt(args[++i]);
                    break;
                case "-paging":
                    hitsPerPage = Integer.parseInt(args[++i]);
                    if (hitsPerPage <= 0) {
                        System.err.println("There must be at least 1 hit per page.");
                        System.exit(1);
                    }
                    break;
                case "-queries":
                    str = args[++i];
                    if(str.equals("all")){
                        break;
                    }else if(str.contains("-")){
                        parts = str.split("-");
                        start = Integer.parseInt(parts[0]);
                        fin = Integer.parseInt(parts[1]);
                    }else{
                        start = Integer.parseInt(str);
                        fin = start;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        new File("output").mkdirs();
        String variable="";
        if(searchStr.equals("jm")){
            variable = "lambda";
            search = new LMJelinekMercerSimilarity(value);
        }else if (searchStr.equals("dir")){
            variable = "mu";
            search = new LMDirichletSimilarity(value);
        }else{
            throw new IllegalArgumentException("valor de similarity erróneo");
        }

        String name = "npl."+searchStr+"."+cut+".hits."+variable+"."+value+".q"+str+".txt";
        String name2 = "npl."+searchStr+"."+cut+".hits."+variable+"."+value+".q"+str+".csv";

        SearchDense search1 = new SearchDense(search, top, cut, start, fin,indexPath, name, name2, knnVectors, hitsPerPage);
        search1.run();
    }
}
