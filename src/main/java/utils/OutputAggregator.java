package utils;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by zaid.mohsin on 10/01/2014.
 */
public class OutputAggregator {

    private final ExecutorService executorService = Executors.newFixedThreadPool(16);
    private List<List<String>> loadAggregateList;
    private List<List<String>> transactionAggregateList;

    public static void main(String[] args) {
        OutputAggregator myAgg = new OutputAggregator();
        try {
            myAgg.init();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public void init() throws java.io.IOException, ParseException

    {

        loadAggregateList = Lists.newArrayList();
        transactionAggregateList = Lists.newArrayList();


        List<File> loadFiles = Lists.newArrayList();
        List<File> transactionFiles = Lists.newArrayList();

        List<List<List<String>>> myLoadList = Lists.newArrayList();
        List<List<List<String>>> myTransactionList = Lists.newArrayList();

        File folder = new File("/Users/zaid.mohsin/Dev/ycsboutput");

        File[] files = folder.listFiles();

        //sort the files
        for (File f : files) {

            if (checkLoadFile(f)) loadFiles.add(f);


            if (checkTransactionFile(f)) transactionFiles.add(f);


        }

        //fetch contents from the load files
        for (File f : loadFiles) {
            try {
                final Future<List<List<String>>> thread = executorService.submit(new FileContentParser(f));
                myLoadList.add(thread.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //fetch contents from the transaction files
        for (File f : transactionFiles) {
            try {
                final Future<List<List<String>>> thread = executorService.submit(new FileContentParser(f));
                myTransactionList.add(thread.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        //create the final load aggregate file
        for (List<List<String>> mylist : myLoadList) {
            if (loadAggregateList.isEmpty())
                loadAggregateList = Lists.newArrayList(mylist);
            else
            addToList(mylist, loadAggregateList);
        }


        //create the final transaction aggregate file
        for (List<List<String>> mylist : myTransactionList) {
            if (transactionAggregateList.isEmpty())
                transactionAggregateList = Lists.newArrayList(mylist);
            else
            addToList(mylist, transactionAggregateList);
        }


        generateOutputFile(loadAggregateList, "aggloadfinal.dat");
        generateOutputFile(transactionAggregateList, "aggtransactionfinal.dat");


    }

    public boolean checkLoadFile(File f) {
        return (f.isFile() && f.getName().substring(0, 4).equalsIgnoreCase("load") ? true : false);
    }

    public boolean checkTransactionFile(File f) {
        return (f.isFile() && f.getName().length() > 12 && f.getName().substring(0, 12).equalsIgnoreCase("transactions") ? true : false);
    }

    public void addToList(List<List<String>> myList, List<List<String>> finalList) throws ParseException{

        List<String> listOfAverageLabels = Lists.newArrayList("RunTime(ms)", "Throughput(ops/sec)", "AverageLatency(us)", "MinLatency(us)", "MaxLatency(us)");


        //add the values
        DecimalFormat myDecimalFormat = new DecimalFormat();
        myDecimalFormat.setParseBigDecimal(true);

        for (int i = 0; i < myList.size(); i++) {



            if (listOfAverageLabels.contains(myList.get(i).get(1).trim()))
                finalList.get(i).set(2,getAvg((BigDecimal) myDecimalFormat.parse(finalList.get(i).get(2).trim()), (BigDecimal) myDecimalFormat.parse(myList.get(i).get(2).trim())));

            else
                //aggregate the value
                finalList.get(i).set(2, getAgg((BigDecimal) myDecimalFormat.parse(finalList.get(i).get(2).trim()), (BigDecimal) myDecimalFormat.parse(myList.get(i).get(2).trim())));
        }

    }

    public void generateOutputFile(List<List<String>> myFinalList, String name) throws IOException {
        PrintWriter myWriter = new PrintWriter(new File("/Users/zaid.mohsin/Dev/ycsboutput/" + name));

        for (int i = 0; i < myFinalList.size(); i++) {
            myWriter.println(myFinalList.get(i).get(0).trim() + ", " + myFinalList.get(i).get(1).trim() + ", " + myFinalList.get(i).get(2).trim());
        }

        myWriter.flush();
        myWriter.close();
    } 
    public String getAvg(BigDecimal first, BigDecimal second)
    {
        first = first.add(second);
        first.divide(new BigDecimal(2));
        return first.toString();
    }

    public String getAgg(BigDecimal first, BigDecimal second)
    {
        first = first.add(second);

        return first.toString();
    }


}
