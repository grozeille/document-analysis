package grozeille;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Supplier;

/**
 * Created by Mathias on 22/02/2015.
 */
public class AnalyzerUtils {

    private static final HashMap<String, Supplier<StopwordAnalyzerBase>> LANG_ANALYZERS;

    static {

        LANG_ANALYZERS = new HashMap<>();

        // FR
        final CharArraySet frenchStopWords = getFrenchStopWords();

        LANG_ANALYZERS.put("fr", () -> new FrenchAnalyzer(frenchStopWords));

        // EN
        final CharArraySet englishStopWords = getEnglishStopWords();
        LANG_ANALYZERS.put("en", () -> new EnglishAnalyzer(englishStopWords));
    }

    public static StopwordAnalyzerBase getAnalyzerByLanguage(String code){

        StopwordAnalyzerBase analyzer;
        Supplier<StopwordAnalyzerBase> analyzerSupplier = LANG_ANALYZERS.get(code);
        if(analyzerSupplier == null){
            analyzer = new StandardAnalyzer();
            System.out.println("Unknown language: "+code);
        }
        else {
            analyzer = analyzerSupplier.get();
        }

        return analyzer;
    }

    private static CharArraySet getFrenchStopWords() {
        CharArraySet frenchStopWords;
        try {
            frenchStopWords = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, "french_stop.txt", StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        }

        return frenchStopWords;
    }

    private static CharArraySet getEnglishStopWords() {
        CharArraySet englishStopWords;
        try {
            englishStopWords = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, "english_stop.txt", StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        }

        return englishStopWords;
    }
}
