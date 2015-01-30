package grozeille;

/**
 * Created by mathias on 20/01/2015.
 */
public class WordWithStat {
    private String word;

    private WordStat stats = new WordStat();

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public WordStat getStats() {
        return stats;
    }

    public void setStats(WordStat stats) {
        this.stats = stats;
    }
}
