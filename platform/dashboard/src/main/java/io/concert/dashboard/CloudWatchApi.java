package io.concert.dashboard;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

/**
 * The two CloudWatch metric calls the dashboard makes, over CloudWatch's Query protocol (form-encoded, SigV4).
 *
 * <p>Why not the SDK's {@code CloudWatchClient}: current SDK versions speak Smithy RPC v2 CBOR to CloudWatch, which
 * LocalStack 4.12 routes but cannot parse (HTTP 500 "An unknown error occurred when trying to parse the request").
 * The Query protocol is what the AWS CLI uses and works the same against LocalStack and real AWS.
 */
final class CloudWatchApi {
    private static final Set<String> RESTRICTED = Set.of("host", "content-length", "connection", "expect", "upgrade");

    /** A metric datum: either {@code value} or {@code stats} = {sampleCount, sum, min, max}. */
    record Datum(String name, Map<String, String> dims, Instant ts, String unit, Double value, double[] stats, int resolution) {}

    /** One GetMetricData query. */
    record Query(String id, String namespace, String name, Map<String, String> dims, int period, String stat) {}

    private final URI endpoint;
    private final String region;
    private final AwsCredentialsProvider credentials;
    private final AwsV4HttpSigner signer = AwsV4HttpSigner.create();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    /** @param endpoint LocalStack's URL, or {@code null} for {@code https://monitoring.<region>.amazonaws.com/} */
    CloudWatchApi(String endpoint, String region, AwsCredentialsProvider credentials) {
        this.endpoint = URI.create(endpoint != null ? endpoint.replaceAll("/+$", "") + "/" : "https://monitoring." + region + ".amazonaws.com/");
        this.region = region;
        this.credentials = credentials;
    }

    void putMetricData(String namespace, List<Datum> datums) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("Action", "PutMetricData");
        p.put("Version", "2010-08-01");
        p.put("Namespace", namespace);
        for (int i = 0; i < datums.size(); i++) {
            Datum d = datums.get(i);
            String m = "MetricData.member." + (i + 1) + ".";
            p.put(m + "MetricName", d.name());
            dims(p, m, d.dims());
            p.put(m + "Timestamp", d.ts().toString());
            p.put(m + "Unit", d.unit());
            p.put(m + "StorageResolution", String.valueOf(d.resolution()));
            if (d.stats() != null) {
                p.put(m + "StatisticValues.SampleCount", num(d.stats()[0]));
                p.put(m + "StatisticValues.Sum", num(d.stats()[1]));
                p.put(m + "StatisticValues.Minimum", num(d.stats()[2]));
                p.put(m + "StatisticValues.Maximum", num(d.stats()[3]));
            } else {
                p.put(m + "Value", num(d.value()));
            }
        }
        call(p);
    }

    /** Datapoints by query id: {@code [epochMillis, value]}, as returned (ascending). Follows NextToken. */
    Map<String, List<double[]>> getMetricData(List<Query> queries, Instant start, Instant end) {
        Map<String, List<double[]>> out = new LinkedHashMap<>();
        String token = null;
        int pages = 0;
        do {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("Action", "GetMetricData");
            p.put("Version", "2010-08-01");
            p.put("StartTime", start.toString());
            p.put("EndTime", end.toString());
            p.put("ScanBy", "TimestampAscending");
            if (token != null) {
                p.put("NextToken", token);
            }
            for (int i = 0; i < queries.size(); i++) {
                Query q = queries.get(i);
                String m = "MetricDataQueries.member." + (i + 1) + ".";
                p.put(m + "Id", q.id());
                p.put(m + "ReturnData", "true");
                p.put(m + "MetricStat.Period", String.valueOf(q.period()));
                p.put(m + "MetricStat.Stat", q.stat());
                p.put(m + "MetricStat.Metric.Namespace", q.namespace());
                p.put(m + "MetricStat.Metric.MetricName", q.name());
                dims(p, m + "MetricStat.Metric.", q.dims());
            }
            Element root = call(p);
            for (Element r : children(first(root, "MetricDataResults"), "member")) {
                String id = text(first(r, "Id"));
                List<Element> ts = children(first(r, "Timestamps"), "member");
                List<Element> vs = children(first(r, "Values"), "member");
                List<double[]> xs = out.computeIfAbsent(id, k -> new ArrayList<>());
                for (int j = 0; j < Math.min(ts.size(), vs.size()); j++) {
                    xs.add(new double[] {Instant.parse(text(ts.get(j))).toEpochMilli(), Double.parseDouble(text(vs.get(j)))});
                }
            }
            token = text(first(root, "NextToken"));
            token = token == null || token.isBlank() ? null : token;
        } while (token != null && ++pages < 20);
        return out;
    }

    private static void dims(Map<String, String> p, String prefix, Map<String, String> dims) {
        int j = 1;
        for (Map.Entry<String, String> e : dims.entrySet()) {
            p.put(prefix + "Dimensions.member." + j + ".Name", e.getKey());
            p.put(prefix + "Dimensions.member." + j + ".Value", e.getValue());
            j++;
        }
    }

    private static String num(double v) {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? String.valueOf((long) v) : String.valueOf(v);
    }

    /** Signs and posts one Query call; returns the response's document element, or throws with CloudWatch's message. */
    private Element call(Map<String, String> params) {
        StringBuilder body = new StringBuilder();
        params.forEach((k, v) -> body.append(body.isEmpty() ? "" : "&").append(enc(k)).append('=').append(enc(v)));
        String payload = body.toString();
        SdkHttpRequest req = SdkHttpRequest.builder().method(SdkHttpMethod.POST).uri(endpoint)
                .putHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8").build();
        SignedRequest signed = signer.sign(r -> r.identity(credentials.resolveCredentials()).request(req)
                .payload(ContentStreamProvider.fromUtf8String(payload))
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "monitoring")
                .putProperty(AwsV4HttpSigner.REGION_NAME, region));
        HttpRequest.Builder b = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
        signed.request().headers().forEach((k, vs) -> {
            if (!RESTRICTED.contains(k.toLowerCase())) {
                vs.forEach(v -> b.header(k, v));
            }
        });
        try {
            HttpResponse<byte[]> r = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            Element root = parse(r.body());
            if (r.statusCode() / 100 != 2) {
                String msg = root == null ? null : text(first(root, "Message"));
                throw new IllegalStateException("CloudWatch " + params.get("Action") + " failed (HTTP " + r.statusCode() + "): "
                        + (msg != null ? msg : new String(r.body(), StandardCharsets.UTF_8)));
            }
            return root;
        } catch (java.io.IOException e) {
            throw new IllegalStateException("CloudWatch unreachable at " + endpoint + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
    }

    static Element parse(byte[] xml) {
        if (xml == null || xml.length == 0) {
            return null;
        }
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setNamespaceAware(false);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            return f.newDocumentBuilder().parse(new ByteArrayInputStream(xml)).getDocumentElement();
        } catch (Exception e) {
            return null;
        }
    }

    /** The first descendant element with this name, or {@code null}. */
    static Element first(Element e, String name) {
        if (e == null) {
            return null;
        }
        NodeList l = e.getElementsByTagName(name);
        return l.getLength() == 0 ? null : (Element) l.item(0);
    }

    /** Direct child elements with this name. */
    static List<Element> children(Element e, String name) {
        List<Element> out = new ArrayList<>();
        if (e == null) {
            return out;
        }
        for (Node n = e.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n instanceof Element c && c.getTagName().equals(name)) {
                out.add(c);
            }
        }
        return out;
    }

    static String text(Element e) {
        return e == null ? null : e.getTextContent().strip();
    }
}
