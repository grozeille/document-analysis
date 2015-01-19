package grozeille;

/**
 * Created by Mathias on 11/01/2015.
 */
public class FileItem {
    private byte[] body;

    private String path;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
