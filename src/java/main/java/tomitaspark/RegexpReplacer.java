package tomitaspark;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONTokener;
import org.json.JSONException;

/**
 * Класс осуществляет замену в строке по списку регулярных выражений, опреденных в json файле
 */
public class RegexpReplacer implements ProcessorInterface<String, String> {
    /**
     * Список замен
     */
    private final Map<Pattern, String> map = new LinkedHashMap<>();

    public static String readFile(String filename) {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Конструктор с указанием пути до json-файла со списком замен
     * @param json_path Путь до файла с заменами
     * @throws FileNotFoundException
     * @throws JSONException
     */
    public RegexpReplacer(String json_path) throws FileNotFoundException {
        // Below does not work on our spark cluster (2.1), but works in 2.4.3
        // This issue is known as dependency hell problem, where some of spark jars depends on older
        // versions of org.json
        //InputStream is = new FileInputStream(json_path);
        //JSONTokener jt = new JSONTokener(is);

        String regexps = readFile(json_path);
        JSONTokener jt = new JSONTokener(regexps);
        JSONArray ja = new JSONArray(jt);

//        for (Object o: ja) {
//            JSONObject replaces = (JSONObject) o;
        for (int i = 0; i < ja.length(); ++i) {
            JSONObject replaces = ja.getJSONObject(i);
            Iterator<String> iter = replaces.keys();
            while (iter.hasNext()) {
                String key = iter.next();
                String value = (String) replaces.get(key);

                map.put(Pattern.compile(key), value);
            }
        }

    }

    /**
     * Осуществляет замену по списку замен
     * @param input Входная строка для замены
     * @return Выходная строка с осуществленными заменами
     */
    public String parse(String input) {
        for (Map.Entry<Pattern, String> entry: map.entrySet())
            input = entry.getKey().matcher(input).replaceAll(entry.getValue());
        return input;
    }

    /**
     * Выводит в консоль список замен
     */
    public void describe() {
        for (Map.Entry<Pattern, String> entry: map.entrySet())
            System.out.println(entry.getKey().pattern() + " -> " + entry.getValue());
    }

    /**
     * "Деструктор" для очистки
     */
    public void dispose() {
    }
}
