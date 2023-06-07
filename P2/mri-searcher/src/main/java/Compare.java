import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Compare {
    private static double[] loadDocument(String file) throws IOException {
        List<Double> a = new ArrayList<>();
        InputStream stream = Files.newInputStream(Paths.get(file));
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, "UTF-8"));
        try{
            String line;
            int i= 1;
            while (((line = reader.readLine()) != null)) {
                if(i!=1){
                    a.add(Double.parseDouble(line.split(",")[1]));
                }
                i++;
            }
        }finally{
            reader.close();
        }
        double[] b = new double[a.size()];
        int i = 0;
        for(double n :a){
            b[i] = n;
        }
        return b;
    }
    public static void main(String[] args) throws IOException {
        String test="";
        float alpha=0;
        String results1="", results2="";
        String[] parts1,parts2;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-test":
                    test = args[++i];
                    alpha = Float.parseFloat(args[++i]);
                    break;
                case "-results":
                    results1 = args[++i];
                    results2 = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        new File("output").mkdirs();
        parts1 = results1.split("\\.");
        parts2 = results2.split("\\.");
        for(int i = 3;i<parts1.length;i++){
            if(!parts1[i].equals(parts2[i])){
                throw new IllegalArgumentException("result1 y result2 no son compatibles");
            }
        }
        double[] a = loadDocument("output/"+results1);
        double[] b = loadDocument("output/"+results2);
        double p=0;
        if(test.equals("t")){
            p = new TTest().pairedTTest(a, b);

        }else if (test.equals("wilcoxon")){
            p = new WilcoxonSignedRankTest().wilcoxonSignedRankTest(a, b, true);
        }
        System.out.println("valor de p = "+p+"\nvalor de alpha = "+alpha);
        if(p<=alpha){
            System.out.println("Rechazamos la hipótesis nula, uno es mejor que el otro");
        }else{
            System.out.println("No rechazamos la hipótesis nula, ninguno es mejor");
        }
    }
}
