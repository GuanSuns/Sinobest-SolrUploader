package org.lin.solr.uploader;

import com.csvreader.CsvReader;
import com.sun.deploy.util.SessionState;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.lin.solr.uploader.config.SolrjConfig;
import org.lin.solr.uploader.utils.Update;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

public class Uploader {

    public static void main(String[] args) {
        SolrClient client;
        SolrClient clientSuggestion;
        try{
            client = new HttpSolrClient.Builder(SolrjConfig.solrNewsURL).build();
            clientSuggestion = new HttpSolrClient.Builder(SolrjConfig.solrSuggestionURL).build();
        }catch (Exception e){
            e.printStackTrace();
            return;
        }

        addSuggestion(clientSuggestion);
    }

    private static void addNews(SolrClient client){
        CsvReader r = null;
        try{
            r = new CsvReader("C:\\Users\\guanl\\Documents\\Jupyter Notebook\\Kaggle\\All News\\articles2_parsing.csv", ',', Charset.forName("utf-8"));
        }catch (Exception e){
            e.printStackTrace();
        }

        if(r != null){
            try{
                r.setSafetySwitch(false);
                r.readHeaders();
                int nDoc = 0;

                while (r.readRecord()) {
                    nDoc++;

                    if(nDoc > 0){
                        Map<String, String> doc1 = new LinkedHashMap<>();
                        String strPublisher = r.get("publisher");
                        String strContent = r.get("content");
                        String strId = r.get("id");
                        String strTitle = r.get("title");

                        if(strContent.length() < 100000){
                            doc1.put("publisher", strPublisher);
                            doc1.put("content", strContent);
                            doc1.put("id", strId);
                            doc1.put("title", strTitle);

                            Update solrUpdate = new Update(client);

                            solrUpdate.add(doc1);
                            client.commit(true, true);

                            System.out.println(nDoc + ". " + "Add Doc - id: " + strId
                                    + ", title: " + strTitle
                                    + ", publisher: " + strPublisher);
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private static void addSuggestion(SolrClient client){
        String filename = "C:\\Users\\guanl\\Documents\\Jupyter Notebook\\Kaggle\\All News\\result_rules.txt";
        try{
            FileInputStream in = new FileInputStream(filename);
            InputStreamReader inReader = new InputStreamReader(in, "UTF-8");
            BufferedReader bufReader = new BufferedReader(inReader);
            String line;

            int i = 1;
            while((line = bufReader.readLine()) != null){
                Map<String, String> doc1 = new LinkedHashMap<>();
                doc1.put("terms", line);
                doc1.put("id", i + "");

                System.out.println(i + " - " + line);
                Update solrUpdate = new Update(client);

                solrUpdate.add(doc1);
                client.commit(true, true);

                i++;
            }

            bufReader.close();
            inReader.close();
            in.close();
        }catch(Exception e){
            e.printStackTrace();
            return;
        }
    }
}
