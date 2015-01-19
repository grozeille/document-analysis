package grozeille;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Mathias on 11/01/2015.
 */
public class DocumentExtractor implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentExtractor.class);

    private transient Parser parser;
    private transient PdfTitleExtractor pdfTitleExtractor;

    public Map<String, Object> extract(String fileName, InputStream stream) throws IOException {

        parser = new AutoDetectParser();
        pdfTitleExtractor = new PdfTitleExtractor();

        Map<String, Object> result = new HashMap<String, Object>();
        Path file = Paths.get(fileName);
        result.put("path", file.toString());
        result.put("file", file.getFileName().toString());

        try {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ContentHandler handler = new BodyContentHandler(bos);


            Metadata metadata = new Metadata();
            parser.parse(stream, handler, metadata, new ParseContext());

            for (String name : metadata.names()) {
                String value = metadata.get(name);

                if (value != null) {
                    result.put(name, value);
                }
            }

            result.put("body", bos.toString());

            String contentType = metadata.get("Content-Type");

            // essaye d'extraire le titre de la première page
            if(contentType.equals("application/pdf")){
                stream.reset();
                List<String> titles = pdfTitleExtractor.findTitles(stream);
                int cptTitle = 0;
                for(String title : titles) {
                    result.put("parsed_title_" + cptTitle, title);
                    cptTitle++;
                }

                stream.reset();
                PDDocument document = PDDocument.load(stream);
                result.put("pages", document.getNumberOfPages());
            }
            // TODO: documents Word

        } catch (Exception e) {
            result.put("exception", e.toString());
            LOG.error("Unable to parse document "+fileName, e);
        }

        return result;
    }
}
