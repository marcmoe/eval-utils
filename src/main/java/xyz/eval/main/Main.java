package xyz.eval.main;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;

import xyz.eval.es.sync.EsUtil;

/**
 *
 * @author Y. Petrick
 */
public class Main {

    public static void main(final String[] args) throws ElasticsearchException, IOException {
        EsUtil.dump(EsUtil.buildClient("elasticsearch", "localhost", 9300), "c69de5c4-3ae0-458b-ab6c-98c3a7c28a4b", "dump.json");
    }
}
