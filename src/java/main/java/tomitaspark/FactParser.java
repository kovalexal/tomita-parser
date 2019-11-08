package tomitaspark;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FactParser implements ProcessorInterface<String, String[]> {

    public String[] parse(String input) {
        List<String> extractedFacts = new LinkedList<String>();

        try {
            JSONObject xmlJSONObj = XML.toJSONObject(input)
                    .getJSONObject("fdo_objects")
                    .getJSONObject("document")
                    .getJSONObject("facts");

            // Iterate over all fact types
            Iterator<String> keysIterator = xmlJSONObj.keys();
            while (keysIterator.hasNext()) {
                String factType = keysIterator.next();

                // Get fact key
                // Detect, if it is array or object
                Object factKey = xmlJSONObj.get(factType);
                if (factKey instanceof JSONArray) {
                    JSONArray factArray = (JSONArray) factKey;

                    for (int i = 0; i < factArray.length(); ++i) {
                        JSONObject factObject = factArray.getJSONObject(i);
                        JSONObject resultFact = parseSingleFact(factObject);
                        resultFact.put("NFactType", factType);
                        extractedFacts.add(resultFact.toString());
                    }

                    System.out.println("Array");
                } else if (factKey instanceof JSONObject) {
                    JSONObject factObject = (JSONObject) factKey;
                    JSONObject resultFact = parseSingleFact(factObject);
                    resultFact.put("NFactType", factType);
                    extractedFacts.add(resultFact.toString());
                } else {
                    throw new JSONException("Value of " + factType + " is not an array or object");
                }
            }

        } catch (JSONException je) {
        }

        return (String[]) extractedFacts.toArray(new String[extractedFacts.size()]);
    }

    private static JSONObject parseSingleFact(JSONObject factObject) {
        JSONObject resultFact = new JSONObject();

        // Iterate over all keys
        Iterator<String> descIterator = factObject.keys();
        while (descIterator.hasNext()) {
            String descKey = descIterator.next();
            Object descValue = factObject.get(descKey);
            if ((descValue instanceof JSONObject)) {
                JSONObject descValueJson = (JSONObject) descValue;
                if (descValueJson.has("val"))
                    resultFact.put(descKey, descValueJson.get("val"));
            }
        }

        return resultFact;
    }

    public void dispose() {
    }
}
