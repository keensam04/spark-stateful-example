import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Optional;

/**
 * Helper class to manage VCAP_SERVICES environment value
 */

@SuppressWarnings("ALL")
public class VcapServices {

    private final JsonObject vcapService;

    @SuppressWarnings("unused")
    VcapServices(String env) {
        vcapService = new Gson().fromJson(env, JsonObject.class);

    }

    /**
     * Get a json object with credentials for CF build-in services (e.g. redis)
     *
     * @param type String with the name/type of the required services credentials
     * @return a json object with credentials
     */
    @SuppressWarnings("unused")
    public JsonObject getCredentialsByType(String type) {
        if (vcapService == null) return null;
        JsonElement element = vcapService.get(type);
        if (element == null) return null;
        return element.getAsJsonArray().get(0).getAsJsonObject().get("credentials").getAsJsonObject();
    }

    /**
     * Get a json object with credentials for a component in CF (e.g Cassandra / Hana / Redis)
     *
     * @param name String with the name of the required service
     *             Compares name with field "name" in VCAP services
     * @return a json object with credentials
     */
    public JsonObject getCredentialsByName(String name) {
        if (vcapService == null) return null;
        final JsonObject[] credentials = new JsonObject[1];

        //There can be two types of elements:
        // userprovided which contains external components like cassandra
        // or redis

        vcapService.entrySet().forEach(entry -> {
            if (entry.getKey().equals("user-provided") && entry.getValue().isJsonArray()) {
                entry.getValue().getAsJsonArray().forEach(userProvElement -> {
                    if (userProvElement.getAsJsonObject().get("name").getAsString().equals(name))
                        credentials[0] = userProvElement.getAsJsonObject().get("credentials").getAsJsonObject();
                });
            } else if (entry.getValue().isJsonArray()) {
                JsonArray jsonArray = entry.getValue().getAsJsonArray();
                if (jsonArray.size() == 1) {
                    JsonObject el = jsonArray.get(0).getAsJsonObject();
                    JsonElement n = el.get("name");
                    if (n != null && n.getAsString().equals(name)) {
                        credentials[0] = el.get("credentials").getAsJsonObject();
                    }
                }
            }
        });
        if (credentials[0] == null) return null;
        return credentials[0];
    }


    public JsonObject getAllCredentialsByType(String type) {
        if (vcapService == null) return null;
        JsonElement element = vcapService.get(type);
        if (element == null) return null;

        final JsonArray allCredentials = element.getAsJsonArray();
        final JsonObject allAredentialsByType = new JsonObject();
        for (JsonElement e1 : allCredentials) {
            final JsonObject jsonObject = e1.getAsJsonObject();
            final String serviceName = jsonObject.get("name").getAsString();
            allAredentialsByType.add(serviceName, jsonObject.get("credentials").getAsJsonObject());
        }
        return allAredentialsByType;
    }

    /***
     * Provides the instance (singleton) for VCAP_SERVICES
     *
     * @return the instance or empty if VCAP_SERVICES are not set in environment variable VCAP_SERVICES
     */
    @SuppressWarnings("unused")
    public static Optional<VcapServices> getInstance() {
        String env = System.getenv("VCAP_SERVICES");
        return (env == null || env.equals("{}") || env.isEmpty()) ? Optional.empty() : Optional.of(new VcapServices
                (env));

    }

    public static String jsonArrayToConnection(final JsonArray array) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.size(); i++) {
            final JsonObject element = array.get(i).getAsJsonObject();
            if (i > 0) sb.append(",");
            sb.append(element.get("hostname").getAsString()).append(":").append(element.get("port").getAsInt());
        }
        return sb.toString();
    }

}