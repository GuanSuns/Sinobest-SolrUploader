package org.lin.solr.uploader.utils;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by guanl on 6/7/2017.
 */
public class Update {
    private SolrClient client;

    public Update(){
        client = null;
    }

    public Update(SolrClient client){
        this.client = client;
    }

    public void setClient(SolrClient client){
        this.client = client;
    }

    public boolean add(Map<String,String> doc) throws Exception{

        if(client == null || doc == null){
            return false;
        }

        SolrInputDocument new_doc = new SolrInputDocument();

        for(Map.Entry doc_entry : doc.entrySet()){
            if(doc_entry == null){
                return false;
            }
            String field_name = doc_entry.getKey().toString();
            String field_value = doc_entry.getValue().toString();
            new_doc.addField(field_name, field_value);
        }

        client.add(new_doc);
        return true;
    }

    public boolean deleteByQuery(Map<String,String> queryDoc) throws Exception{
        if(client == null || queryDoc == null){
            return false;
        }

        String queryString = "";
        Iterator entries = queryDoc.entrySet().iterator();
        int n_query = queryDoc.size();
        for(int i=0; i<n_query; i++){
            Map.Entry entry = (Map.Entry) entries.next();
            if(i == 0){
                queryString = entry.getKey().toString()
                        + ":"
                        + entry.getValue().toString();
            }else{
                queryString = queryString
                        + " "
                        + entry.getKey().toString()
                        + ":"
                        + entry.getValue().toString();
            }
        }

        client.deleteByQuery(queryString);
        return true;
    }

    public boolean deleteById(String id) throws Exception{
        if(client == null || id == null){
            return false;
        }

        client.deleteById(id);
        return true;
    }
}
