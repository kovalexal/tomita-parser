package tomitaspark;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;

import com.srg.tomita.TomitaParser;

public class TomitaParserProcessor implements ProcessorInterface<String, String> {
    private TomitaParser parser;

    public TomitaParserProcessor(String configPath) {
        String cwd = System.getProperty("user.dir");
        String mystemPath = Paths.get(cwd, "libmystem_c_binding.so").toString();
        String factExtractPath = Paths.get(cwd, "libFactExtract-Parser-textminerlib_java.so").toString();

        System.load(mystemPath);
        System.load(factExtractPath);

        File configFile = new File(configPath);

        parser = new TomitaParser(
                new File(configFile.getParent()),
                new String[]{ configFile.getName() }
        );
    }

    public String parse(String input) {
        Collection<String> collection = new LinkedList<String>();
        collection.add(input);
        return parser.parse(collection);
    }

    public void dispose() {
        parser.dispose();
    }
}
