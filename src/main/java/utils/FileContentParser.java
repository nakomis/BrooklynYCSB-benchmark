package utils;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by zaid.mohsin on 10/01/2014.
 */
public class FileContentParser implements Callable<List<List<String>>> {

    private List<List<String>> outputFields;
    private File file;

    public FileContentParser(File file) {
        this.file = file;
        outputFields = Lists.newArrayList();
    }

    @Override
    public List<List<String>> call() throws java.io.IOException {


        BufferedReader filereader = new BufferedReader(new FileReader(file));
        //skip two lines
        filereader.readLine();
        filereader.readLine();


        String line;
        while ((line = filereader.readLine()) != null) {

            outputFields.add(Lists.newArrayList(line.trim().split(",")));

        }
        return outputFields;
    }

    public List<List<String>> getFileAsArrayList() {
        return outputFields;
    }
}
