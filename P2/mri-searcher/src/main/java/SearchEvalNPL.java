

import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.File;

public class SearchEvalNPL {
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
        String str = "";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-search":
                    searchStr = args[++i];
                    value = Float.parseFloat(args[++i]);
                    break;
                case "-indexin":
                    indexPath = args[++i];
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
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

        Search search1 = new Search(search, top, cut, start, fin,indexPath, name, name2);
        search1.run();
    }
}
