package utils;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by zaid.mohsin on 10/01/2014.
 */
public class OutputAggregator {

    private static final List<String> LIST_OF_LABELS = Lists.newArrayList("RunTime(ms)", "Throughput(ops/sec)", "AverageLatency(us)", "MinLatency(us)", "MaxLatency(us)");
    private final ExecutorService executorService = Executors.newFixedThreadPool(16);
    private List<List<String>> loadAggregateList;
    private List<List<String>> transactionAggregateList;
    private static final File FOLDER = new File("/Users/zaid.mohsin/Dev/ycsboutput");

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



        File[] files = FOLDER.listFiles();

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


        calculateAverages(loadAggregateList, loadFiles.size());
        calculateAverages(transactionAggregateList, transactionFiles.size());

        generateOutputFile(loadAggregateList, "aggloadfinal.dat");
        generateOutputFile(transactionAggregateList, "aggtransactionfinal.dat");


    }

    public boolean checkLoadFile(File f) {
        return (f.isFile() && f.getName().substring(0, 4).equalsIgnoreCase("load") ? true : false);
    }

    public boolean checkTransactionFile(File f) {
        return (f.isFile() && f.getName().length() > 12 && f.getName().substring(0, 12).equalsIgnoreCase("transactions") ? true : false);
    }

    public void addToList(List<List<String>> myList, List<List<String>> finalList) throws ParseException {


        //add the values
        DecimalFormat df = new DecimalFormat();
        df.setParseBigDecimal(true);

        for (int i = 0; i < myList.size(); i++) {


            finalList.get(i).set(2, getAgg((BigDecimal) df.parse(finalList.get(i).get(2).trim()), (BigDecimal) df.parse(myList.get(i).get(2).trim())));
        }

    }

    public static void generateOutputFile(List<List<String>> myFinalList, String name) throws IOException {
        PrintWriter myWriter = new PrintWriter(new File(FOLDER.getAbsolutePath() + "/" + name));

        for (int i = 0; i < myFinalList.size(); i++) {
            myWriter.println(myFinalList.get(i).get(0).trim() + ", " + myFinalList.get(i).get(1).trim() + ", " + myFinalList.get(i).get(2).trim());
        }

        myWriter.flush();
        myWriter.close();
    }

    public String getAgg(BigDecimal first, BigDecimal second) {
        first = first.add(second);

        return first.toString();
    }

    public String getAvg(BigDecimal value, int factor) {
        value = value.divide(new BigDecimal(factor));

        return value.toString();
    }

    public void calculateAverages(List<List<String>> myFinalList, int numOfFiles) throws ParseException {


        DecimalFormat df = new DecimalFormat();
        df.setParseBigDecimal(true);

        for (int i = 0; i < myFinalList.size(); i++) {
            if (LIST_OF_LABELS.contains(myFinalList.get(i).get(1).trim())) {
                myFinalList.get(i).set(2, getAvg((BigDecimal) df.parse(myFinalList.get(i).get(2)), numOfFiles));
            }
        }
    }


}
