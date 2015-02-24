package grozeille;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.tika.language.LanguageIdentifier;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
* Created by Mathias on 16/02/2015.
*/
public class TokenStreamIterable implements Iterable<String>{

    private TokenStream tokenStream;
    private OffsetAttribute offsetAttribute;
    private CharTermAttribute charTermAttribute;

    TokenStreamIterable(String text) throws IOException {
        this(text, AnalyzerUtils.getAnalyzerByLanguage(new LanguageIdentifier( text.toString()).getLanguage()), null);
    }

    TokenStreamIterable(String text, Analyzer analyzer) throws IOException {
        this(text, analyzer, null);
    }

    TokenStreamIterable(String text, Analyzer analyzer, Function<TokenStream, TokenStream> postFilters) throws IOException {

        tokenStream = analyzer.tokenStream("", new StringReader(text));
        if(postFilters != null) {
            tokenStream = postFilters.apply(tokenStream);
        }

        offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
        charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        tokenStream.reset();
    }

    private class FrenchTokenStreamIterator implements Iterator<String> {

        private boolean hasNext = true;

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public String next() {
            try {
                hasNext = tokenStream.incrementToken();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(hasNext) {
                int startOffset = offsetAttribute.startOffset();
                int endOffset = offsetAttribute.endOffset();
                return charTermAttribute.toString();
            }
            else {
                return "";
            }
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new FrenchTokenStreamIterator();
    }
}
