package Util;

import com.alibaba.fastjson.JSON;

public class ToJson {
    public String getJsonString(String kafkaData) {

        if (kafkaData != null && kafkaData.startsWith("\"") && kafkaData.endsWith("\"")) {
            kafkaData = kafkaData.substring(1, kafkaData.length() - 1).replace("\"\"", "\"");
        }

        return String.valueOf(JSON.toJSON(kafkaData));
    }
}
