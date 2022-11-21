package io.openlineage.spark.agent;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
@Slf4j
@Getter
public class UrlParser {

    public static final Set<String> namedParams =
            new HashSet<>(Arrays.asList("timeout", "api_key", "app_name"));
    public static final String disabledFacetsSeparator = ";";

    public String host = "";
    public String version = "v1";
    public String namespace = "default";
    public String jobName = "default";
    public String parentRunId = null;
    public Optional<Double> timeout = Optional.empty();
    public Optional<String> apiKey = Optional.empty();
    public Optional<String> appName = Optional.empty();
    public Optional<Map<String, String>> urlParams = Optional.empty();
    public String disabledFacets = "spark_unknown";
    public boolean consoleMode = false;

    public static ArgumentParser.ArgumentParserBuilder parseUrl(String clientUrl) {
        ArgumentParser.ArgumentParserBuilder builder = ArgumentParser.builder();
        URI uri = URI.create(clientUrl);
        String path = uri.getPath();
        String[] elements = path.split("/");
        List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);

        builder
                .host(uri.getScheme() + "://" + uri.getAuthority())
                .timeout(getTimeout(nameValuePairList))
                .apiKey(getNamedStringParameter(nameValuePairList, "api_key"))
                .appName(getNamedStringParameter(nameValuePairList, "app_name"))
                .urlParams(getUrlParams(nameValuePairList))
                .consoleMode(false);

        get(elements, "api", 1).ifPresent(builder::version);
        get(elements, "namespaces", 3).ifPresent(builder::namespace);
        get(elements, "jobs", 5).ifPresent(builder::jobName);
        get(elements, "runs", 7).ifPresent(builder::parentRunId);
        return builder;
    }
    
    public static UUID getRandomUuid() {
        return UUID.randomUUID();
    }

    private static Optional<String> getNamedStringParameter(
            List<NameValuePair> nameValuePairList, String name) {
        return Optional.ofNullable(getNamedParameter(nameValuePairList, name))
                .filter(StringUtils::isNoneBlank);
    }

    private static Optional<Double> getTimeout(List<NameValuePair> nameValuePairList) {
        return Optional.ofNullable(
                UrlParser.extractTimeout(getNamedParameter(nameValuePairList, "timeout")));
    }

    private static Double extractTimeout(String timeoutString) {
        try {
            if (StringUtils.isNotBlank(timeoutString)) {
                return Double.parseDouble(timeoutString);
            }
        } catch (NumberFormatException e) {
            log.warn("Value of timeout is not parsable");
        }
        return null;
    }

    public String getUrlParam(String urlParamName) {
        String param = null;
        if (urlParams.isPresent()) {
            param = urlParams.get().get(urlParamName);
        }
        return param;
    }

    public String[] extractDisabledFacets() {
        return disabledFacets.split(disabledFacetsSeparator);
    }

    private static Optional<Map<String, String>> getUrlParams(List<NameValuePair> nameValuePairList) {
        final Map<String, String> urlParams = new HashMap<String, String>();
        nameValuePairList.stream()
                .filter(pair -> !namedParams.contains(pair.getName()))
                .forEach(pair -> urlParams.put(pair.getName(), pair.getValue()));

        return urlParams.isEmpty() ? Optional.empty() : Optional.ofNullable(urlParams);
    }

    protected static String getNamedParameter(List<NameValuePair> nameValuePairList, String param) {
        for (NameValuePair nameValuePair : nameValuePairList) {
            if (nameValuePair.getName().equalsIgnoreCase(param)) {
                return nameValuePair.getValue();
            }
        }
        return null;
    }

    private static Optional<String> get(String[] elements, String name, int index) {
        boolean check = elements.length > index + 1 && name.equals(elements[index]);
        if (check) {
            return Optional.of(elements[index + 1]);
        } else {
            log.warn("missing " + name + " in " + Arrays.toString(elements) + " at " + index);
            return Optional.empty();
        }
    }
}
