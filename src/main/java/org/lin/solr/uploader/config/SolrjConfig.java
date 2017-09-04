package org.lin.solr.uploader.config;

/**
 * Created by guanl on 6/7/2017.
 */
public class SolrjConfig {
    public final static String solrAnalysisFilterName = "org.apache.lucene.analysis.core.LowerCaseFilter";
    public final static String engineSearchField = "search_all";
    public final static int rowSize = 20;
    public final static String solrSuggestionURL = "http://localhost:8983/solr/suggestion_core";
    public final static String solrNewsURL = "http://localhost:8983/solr/news_core";
}
