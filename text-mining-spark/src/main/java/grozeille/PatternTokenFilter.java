package grozeille;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by Mathias on 24/02/2015.
 */
public class PatternTokenFilter extends FilteringTokenFilter {

    private final CharTermAttribute termAtt;
    private Pattern pattern;

    public PatternTokenFilter(TokenStream in, Pattern pattern) {
        super(in);
        this.pattern = pattern;
        this.termAtt = (CharTermAttribute)this.addAttribute(CharTermAttribute.class);
    }


    @Override
    protected boolean accept() throws IOException {
        return pattern.matcher(termAtt).matches();
    }
}
