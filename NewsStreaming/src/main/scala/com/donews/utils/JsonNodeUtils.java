package com.donews.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;

import java.util.Set;

public class JsonNodeUtils {

    public static JsonNode getJsonNodeFromStringContent(String content) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(content);
    }

    public static JsonSchema getJsonSchemaFromStringContent(String schemaContent) {
        JsonSchemaFactory factory = new JsonSchemaFactory();
        return factory.getSchema(schemaContent);
    }

    public static String validateData(ObjectNode node, String schema_str) {
        JsonSchema schema = getJsonSchemaFromStringContent(schema_str);
        Set<ValidationMessage> messages = schema.validate(node);
        if (messages.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (ValidationMessage msg : messages) {
                sb.append(msg.getMessage()).append(",");
            }
            sb.append("}");
            return sb.toString();
        } else return "";
    }
}
