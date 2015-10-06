package xyz.eval.es.sync;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

/**
 *
 * @author Y. Petrick
 */
public class EsUtil {
    private static final Logger LOG = Logger.getLogger(EsUtil.class.getSimpleName());

    public static Client buildClient(final String cluster, final String host, final int port) {
        final TransportClient client = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", cluster));
        client.addTransportAddress(new InetSocketTransportAddress(host, port));
        return client;
    }

    public static Node buildEsNode(final String cluster, final String host, final String port) {
        final Node node = NodeBuilder.nodeBuilder() //
                .clusterName(cluster) //
                .local(false)//
                .settings(ImmutableSettings.settingsBuilder()//
                        .put("node.name", "node-test-" + System.currentTimeMillis()) //
                        .put("script.disable_dynamic", "sandbox") //
                        //                        .put("path.data", dataDir) //
                        //                        .put("path.logs", "target/elasticsearch/logs2") //
                        .put("gateway.type", "none") //
                        .put("index.store.type", "memory") //
                        .put("index.store.fs.memory.enabled", true) //
                        .put("index.number_of_shards", 1) //
                        .put("index.number_of_replicas", 0) //
                        .put("transport.host", host).put("transport.tcp.port", port).build()).node();
        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        return node;
    }

    public static Node buildNodeFromDump(final String dumpPath) throws IOException, Exception {
        final Node node = EsUtil.buildEsNode("test-dump", "localhost", "5555");
        EsUtil.read(node.client(), dumpPath);
        return node;
    }

    private static ImmutableOpenMap<String, MappingMetaData> getMapping(final Client client, final String index) {
        final GetMappingsRequestBuilder request = client.admin().indices().prepareGetMappings(index);
        return request.get().getMappings().get(index);
    }

    private static void dumpMappings(final Client client, final String index, final JsonGenerator jgen) throws ElasticsearchException, IOException {
        jgen.writeObjectFieldStart("mappings");
        for (final ObjectObjectCursor<String, MappingMetaData> value : getMapping(client, index)) {
            jgen.writeObjectField(value.value.type(), value.value.sourceAsMap());
        }
        jgen.writeEndObject();

    }

    private static void dumpDocuments(final Client client, final String index, final JsonGenerator jgen, final QueryBuilder query) throws JsonProcessingException, IOException {
        final int timeOut = 5;
        final long countTotalDoc = client.prepareCount(index).get(TimeValue.timeValueMinutes(timeOut)).getCount();
        LOG.info(String.format("Going to dump %d documents", countTotalDoc));

        jgen.writeObjectFieldStart("documents");
        //        final ObjectMapper mapper = new ObjectMapper();
        long progress = 0;
        for (final ObjectObjectCursor<String, MappingMetaData> value : getMapping(client, index)) {
            final long countTotalCurrentType = client.prepareCount(index).setTypes(value.value.type()).get(TimeValue.timeValueMinutes(timeOut)).getCount();
            LOG.info(String.format("Going to dump %d %s documents", countTotalCurrentType, value.value.type()));
            final SearchRequestBuilder rb = client.prepareSearch(index).setTypes(value.value.type()).setSearchType(SearchType.SCAN).setScroll(TimeValue.timeValueMinutes(timeOut)).setQuery(query).setSize(1000);
            SearchResponse scrollResp = rb.execute().actionGet();
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(TimeValue.timeValueMinutes(timeOut)).execute().actionGet();
            jgen.writeArrayFieldStart(value.value.type());
            while (scrollResp.getHits().getHits().length != 0) {
                for (final SearchHit hit : scrollResp.getHits()) {
                    //                    jgen.writeObject(mapper.readValue(hit.sourceAsString(), JsonNode.class));
                    jgen.writeObject(hit.sourceAsMap());
                }
                progress += scrollResp.getHits().hits().length;
                LOG.info(String.format("%d of %d documents remaining", countTotalDoc - progress, countTotalDoc));
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(TimeValue.timeValueMinutes(timeOut)).execute().actionGet();
            }
            jgen.writeEndArray();
        }
        jgen.writeEndObject();
    }

    public static void dump(final Client client, final String index, final String target) throws ElasticsearchException, IOException {
        dump(client, index, target, QueryBuilders.matchAllQuery());
    }

    public static void dump(final Client client, final String index, final String target, final QueryBuilder query) throws ElasticsearchException, IOException {
        try (final JsonGenerator jgen = new ObjectMapper().getFactory().createGenerator(new FileWriter(target))) {
            jgen.writeStartObject();
            jgen.writeObjectFieldStart(index);
            dumpMappings(client, index, jgen);
            dumpDocuments(client, index, jgen, query);
            jgen.writeEndObject();
            jgen.writeEndObject();
        }
    }

    public static final int BATCH_SIZE = 100;

    private static String createIndex(final Client client, final JsonParser jp) throws IOException {
        if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            final ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0);
            client.admin().indices().prepareCreate(jp.getCurrentName()).setSettings(builder).get();
            client.admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout(TimeValue.timeValueMinutes(1)).execute().actionGet();
            return jp.getCurrentName();
        }
        return null;
    }

    private static void persistMappings(final Client client, final String index, final JsonParser jp) throws ElasticsearchException, IOException {
        if (jp.nextToken() == JsonToken.START_OBJECT) {
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                client.admin().indices().preparePutMapping(index).setType(jp.getCurrentName()).setSource(readNextValueAs(new TypeReference<Map<String, Object>>() {
                }, jp)).get();
            }
        }
    }

    private static void persistDocuments(final Client client, final String index, final long count, final JsonParser jp) throws JsonProcessingException, IOException {
        if (jp.nextToken() == JsonToken.START_OBJECT) {
            long cnt = 0;
            boolean empty = false;
            BulkRequestBuilder bulk = client.prepareBulk();
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                final String type = jp.getCurrentName();
                jp.nextToken();
                while (jp.nextToken() != JsonToken.END_ARRAY) {
                    bulk.add(client.prepareIndex(index, type).setSource(readNextValueAs(new TypeReference<Map<String, Object>>() {
                    }, jp)));
                    if (empty = ++cnt % BATCH_SIZE == 0) {
                        LOG.info(String.format("%d of %d documents remaining", count - cnt, count));
                        bulk.get();
                        bulk = client.prepareBulk();
                    }
                }
            }
            if (!empty) {
                bulk.get();
            }
            client.admin().indices().refresh(Requests.refreshRequest()).actionGet();
        }
    }

    private static <T> T readNextValueAs(final TypeReference<T> valueTypeRef, final JsonParser jp) throws JsonParseException, IOException {
        jp.nextToken();
        return jp.<T> readValueAs(new TypeReference<T>() {
        });
    }

    public static void read(final Client client, final String source) throws Exception, IOException {
        LOG.info("Counting documents");
        final Map<String, Long> documentCount = count(client, source);
        try (JsonParser jp = new ObjectMapper().getFactory().createParser(new FileReader(source))) {
            if (jp.nextToken() != JsonToken.START_OBJECT) {
                throw new RuntimeException("invalid json");
            }
            while (jp.nextToken() == JsonToken.FIELD_NAME) { //index
                final String index = createIndex(client, jp);
                LOG.info(String.format("index %s created", index));
                if (jp.nextToken() != JsonToken.START_OBJECT) {
                    throw new RuntimeException("invalid json");
                }
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    switch (jp.getCurrentName()) {
                    case "mappings":
                        persistMappings(client, index, jp);
                        LOG.info("mappings created");
                        break;
                    case "documents":
                        LOG.info(String.format("Going to persist %d documents to %s", documentCount.get(index), index));
                        persistDocuments(client, index, documentCount.get(index), jp);
                        break;
                    default:
                        throw new RuntimeException("unknown filed " + jp.getCurrentName());
                    }
                }
                if (jp.getCurrentToken() != JsonToken.END_OBJECT) {
                    throw new RuntimeException("invalid json");
                }
            }
        }
    }

    public static Map<String, Long> count(final Client client, final String source) throws Exception, IOException {
        final Map<String, Long> documentCount = new HashMap<>();
        try (JsonParser jp = new ObjectMapper().getFactory().createParser(new FileReader(source))) {
            if (jp.nextToken() != JsonToken.START_OBJECT) {
                throw new RuntimeException("invalid json");
            }
            while (jp.nextToken() == JsonToken.FIELD_NAME) { //index
                final String index = jp.getCurrentName();
                documentCount.put(index, 0L);
                if (jp.nextToken() != JsonToken.START_OBJECT) {
                    throw new RuntimeException("invalid json");
                }
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    switch (jp.getCurrentName()) {
                    case "mappings":
                        if (jp.nextToken() == JsonToken.START_OBJECT) {
                            jp.skipChildren();
                        }
                        break;
                    case "documents":
                        if (jp.nextToken() == JsonToken.START_OBJECT) {
                            long cnt = 0;
                            while (jp.nextToken() != JsonToken.END_OBJECT) {
                                jp.nextToken();
                                while (jp.nextToken() != JsonToken.END_ARRAY) {
                                    jp.skipChildren();
                                    cnt++;
                                }
                            }
                            documentCount.put(index, cnt);
                        }
                        break;
                    default:
                        throw new RuntimeException("unknown filed " + jp.getCurrentName());
                    }
                }
                if (jp.getCurrentToken() != JsonToken.END_OBJECT) {
                    throw new RuntimeException("invalid json");
                }
            }
        }
        return documentCount;
    }

    public static BoolFilterBuilder filterfromTo(final BoolFilterBuilder f, final Date d) {
        final long fromTs = getStartOfDay(d).getTime();
        final long toTs = getEndOfDay(d).getTime();
        f.must(FilterBuilders.rangeFilter("firstOcc").gte(fromTs));
        f.must(FilterBuilders.rangeFilter("lastOcc").lte(toTs));
        return f;
    }

    public static BoolFilterBuilder filterMetric(final BoolFilterBuilder f, final String metricId) {
        return f.must(FilterBuilders.termFilter("metricId", "60_999"));
    }


    public static <T> List<T> getForDateAndTypeAndPermissionPath(final Client client, final String index, final String type, final Class<T> clz, final Date d, final String permissionPath) throws JsonParseException, JsonMappingException, IOException {
        final BoolFilterBuilder filter = filterfromTo(FilterBuilders.boolFilter(), d);
        filterMetric(filter, "60_999");
        //        filter.must(FilterBuilders.termFilter("levelDesc", ADAMConfigurationConstants.PARAMETER_DOMAIN));
        filter.must(FilterBuilders.termFilter("permissionPath", permissionPath));
        final CountRequestBuilder countReq = client.prepareCount(index).setTypes(type).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));
        final CountResponse countResp = countReq.execute().actionGet(2, TimeUnit.MINUTES);
        LOG.info("Found " + countResp.getCount() + " matching records");

        if (countResp.getCount() > Integer.MAX_VALUE) {
            throw new RuntimeException("To many hits to validate");
        }

        final SearchResponse response = client.prepareSearch(index).setTypes(type).setSize((int) countResp.getCount()).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter)).get();

        final List<T> result = new ArrayList<>();
        final ObjectMapper om = new ObjectMapper();
        for (final SearchHit hit : response.getHits().getHits()) {
            result.add(om.readValue(hit.getSourceAsString(), clz));
        }
        return result;
    }

    public static Date getEndOfDay(final Date d) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
    }

    public static Date getStartOfDay(final Date d) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

}

