package ly.persona.academic.data.etl.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> String serialize(T data) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(data);
    }

    public static <T> T deserialize(String data, Class<T> clazz) throws JsonProcessingException {
        return data == null ? null : OBJECT_MAPPER.readValue(data, clazz);
    }
}
