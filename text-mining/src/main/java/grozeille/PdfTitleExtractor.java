package grozeille;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.pdfbox.util.TextPosition;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Mathias on 11/01/2015.
 */
public class PdfTitleExtractor implements TitleExtractor {
    private static final List<String> spaceCharacters = Lists.newArrayList("\n", "\r", " ", "\t");
    private static final String spaceCharactersPattern = "[\n\r \t]";

    private class MyPDFTextStripper extends PDFTextStripper {

        public MyPDFTextStripper() throws IOException {
            super();
        }

        public Vector<List<TextPosition>> internalCharactersByArticle() {
            return super.getCharactersByArticle();
        }
    }

    private class TitleAnalysisResult {
        private String text;

        private float absoluteDistanceFromMiddle;

        private float absoluteDistanceFromBestPosition;

        private float maxSize;

        private boolean bold;

        private double distanceFromAverageSize;

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public float getAbsoluteDistanceFromMiddle() {
            return absoluteDistanceFromMiddle;
        }

        public void setAbsoluteDistanceFromMiddle(float absoluteDistanceFromMiddle) {
            this.absoluteDistanceFromMiddle = absoluteDistanceFromMiddle;
        }

        public float getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(float size) {
            this.maxSize = size;
        }

        public boolean isBold() {
            return bold;
        }

        public void setBold(boolean bold) {
            this.bold = bold;
        }

        public double getDistanceFromAverageSize() {
            return distanceFromAverageSize;
        }

        public void setDistanceFromAverageSize(double distanceFromAverageSize) {
            this.distanceFromAverageSize = distanceFromAverageSize;
        }

        public float getAbsoluteDistanceFromBestPosition() {
            return absoluteDistanceFromBestPosition;
        }

        public void setAbsoluteDistanceFromBestPosition(float absoluteDistanceFromBestPosition) {
            this.absoluteDistanceFromBestPosition = absoluteDistanceFromBestPosition;
        }
    }

    @Override
    public List<String> findTitles(InputStream stream) throws Exception {

        PDDocument document = PDDocument.load(stream);

        PDPage firstPage = (PDPage) document.getDocumentCatalog().getAllPages().get(0);
        if(firstPage.getMediaBox() == null){
            return new ArrayList<String>();
        }
        float bestTitlePosition = ((firstPage.getMediaBox().getUpperRightY() - firstPage.getMediaBox().getLowerLeftY()) / 5.0f) * 2.0f;
        float middlePosition = (firstPage.getMediaBox().getUpperRightY() - firstPage.getMediaBox().getLowerLeftY()) / 2.0f;

        double averageFontSize = averageFontSize(document);

        MyPDFTextStripper stripper = new MyPDFTextStripper();

        // analyse seulement la première page
        stripper.setStartPage(1);
        stripper.setEndPage(1);
        stripper.setLineSeparator("\n");
        stripper.setPageStart("");
        stripper.setPageSeparator("");
        stripper.setPageStart("");
        stripper.setPageEnd("");
        stripper.setArticleStart("");
        stripper.setArticleEnd("");

        stripper.setParagraphStart("");
        String charEnd = Character.toString((char) 0);
        stripper.setParagraphEnd(charEnd);

        String content = stripper.getText(document);
        document.close();
        Vector<List<TextPosition>> articles = stripper.internalCharactersByArticle();

        if (content.length() == 0) {
            return Arrays.asList(new String[0]);
        }

        if (!content.substring(0, 1).equals(articles.get(0).get(0).getCharacter())) {
            throw new Exception("Parsed text doesn't match raw text");
        }

        // compare avec les données brutes pour extraire la taille de la police et la position

        String[] split = content.split(charEnd);
        int cptRaw = 0;

        TextPosition[] rawText = Iterables.toArray(Iterables.concat(articles), TextPosition.class);

        List<TitleAnalysisResult> results = new ArrayList<TitleAnalysisResult>();


        for (String line : split) {

            // ignore les lignes vides
            if (line.length() == 0) {
                continue;
            }

            String withoutLineEndings = line.replaceAll(spaceCharactersPattern, "");

            TitleAnalysisResult result = new TitleAnalysisResult();
            result.setText(line);

            // calcul la distance relative au milieu de la page avec le premier caractère
            result.setAbsoluteDistanceFromMiddle(Math.abs(bestTitlePosition - rawText[cptRaw].getY()));
            result.setAbsoluteDistanceFromMiddle(Math.abs(middlePosition - rawText[cptRaw].getY()));


            StringBuilder analyzed = new StringBuilder();
            float maxSize = 0.0f;
            float totalLength = 0.0f;
            int totalBold = 0;

            int currentCptRaw = cptRaw;
            int currentCpt = 0;
            while(currentCpt < withoutLineEndings.length()){

                for(int cptChar = 0; cptChar < rawText[currentCptRaw].getCharacter().length(); cptChar++) {
                    // récupère le prochain caractère à chercher
                    Character c = withoutLineEndings.charAt(currentCpt);
                    Character cRaw = rawText[currentCptRaw].getCharacter().charAt(cptChar);

                    // on ignore les caractères "vides"
                    if (Iterables.contains(spaceCharacters, cRaw.toString())) {
                        continue;
                    }

                    analyzed.append(cRaw);
                    if (!c.equals(cRaw)) {
                        throw new Exception("Not the same text:\n" + withoutLineEndings + "\n\n" + analyzed.toString());
                    }

                    maxSize = Math.max(maxSize, rawText[currentCptRaw].getFontSizeInPt());
                    totalBold += isBold(rawText[currentCptRaw].getFont()) ? 1 : 0;
                    totalLength++;

                    currentCpt++;
                }

                currentCptRaw++;
            }

            // plus de la moitié est en gras ?
            result.setBold(((float) totalBold / totalLength) >= 0.5f);

            // taille moyenne ?
            result.setMaxSize(maxSize);
            result.setDistanceFromAverageSize(result.getMaxSize() - averageFontSize);

            // TODO: recherche une date: s'il y en a une, faible chance que ce soit le titre,
            // c'est souvent la date du document ou de la révision


            results.add(result);

            cptRaw = currentCptRaw;
        }

        // filtre le text trop court
        results = new ArrayList<TitleAnalysisResult>(Collections2.filter(results, new Predicate<TitleAnalysisResult>() {
            @Override
            public boolean apply(TitleAnalysisResult titleAnalysisResult) {
                return titleAnalysisResult.getText().length() > 5;
            }
        }));

        // le text le plus gros
        Ordering<TitleAnalysisResult> sizeOrdering = Ordering.from(new Comparator<TitleAnalysisResult>() {
            @Override
            public int compare(TitleAnalysisResult o1, TitleAnalysisResult o2) {
                return new Float(o1.getMaxSize()).compareTo(o2.getMaxSize());
            }
        });

        // le text le plus centré
        Ordering<TitleAnalysisResult> positionOrdering = Ordering.from(new Comparator<TitleAnalysisResult>() {
            @Override
            public int compare(TitleAnalysisResult o1, TitleAnalysisResult o2) {
                return new Float(o1.getAbsoluteDistanceFromMiddle()).compareTo(o2.getAbsoluteDistanceFromMiddle());
            }
        });

        List<TitleAnalysisResult> topSize = sizeOrdering.greatestOf(results, 3);
        List<TitleAnalysisResult> topTitle = positionOrdering.sortedCopy(topSize);

        return Lists.transform(topTitle, new Function<TitleAnalysisResult, String>() {
            @Override
            public String apply(TitleAnalysisResult titleAnalysisResult) {
                return titleAnalysisResult.getText();
            }
        });
    }

    private double averageFontSize(PDDocument document) throws Exception {
        MyPDFTextStripper stripper = new MyPDFTextStripper();

        if(document.getNumberOfPages() == 1){
            stripper.setStartPage(1);
            stripper.setEndPage(1);
        }
        else {
            stripper.setStartPage(2);
            stripper.setEndPage(Math.min(document.getNumberOfPages(), 10));
        }
        stripper.setLineSeparator("");
        stripper.setPageStart("");
        stripper.setPageSeparator("");
        stripper.setPageStart("");
        stripper.setPageEnd("");
        stripper.setArticleStart("");
        stripper.setArticleEnd("");
        stripper.setParagraphStart("");
        stripper.setParagraphEnd("");

        stripper.getText(document);
        Vector<List<TextPosition>> articles = stripper.internalCharactersByArticle();

        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (List<TextPosition> article : articles) {
            for (TextPosition textPosition : article) {
                if (!Iterables.contains(spaceCharacters, textPosition.getCharacter())) {
                    stats.addValue(textPosition.getFontSizeInPt());
                }
            }
        }

        // median
        return stats.getPercentile(50);
    }

    private boolean isBold(PDFont pdFont) {
        String font;

        if (pdFont.getBaseFont() == null) {
            font = pdFont.getSubType();
        } else {
            font = pdFont.getBaseFont();
        }

        /*
         *  a lot of embedded fonts have names like XCSFS+Times, so remove everything before and
         * including '+'
         */
        final int plusIndex = font.indexOf('+');

        if (plusIndex != -1) {
            font = font.substring(plusIndex + 1, font.length());
        }

        return font.toLowerCase().contains("bold");
    }
}
