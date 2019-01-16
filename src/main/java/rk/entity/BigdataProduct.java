package rk.entity;


public class BigdataProduct {
    private String name;
    private String author;
    private String version;

    public BigdataProduct() {
    }

    public BigdataProduct(String name, String author, String version) {
        this.name = name;
        this.author = author;
        this.version = version;
    }

    @Override
    public String toString() {
        return "BigdataProduct{" +
                "name='" + name + '\'' +
                ", author='" + author + '\'' +
                ", version='" + version + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
