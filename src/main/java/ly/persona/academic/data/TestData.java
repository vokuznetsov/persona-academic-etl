package ly.persona.academic.data;

import java.io.Serializable;
import java.util.Objects;

public class TestData implements Serializable {
    private String key;
    private int value;
    public TestData() {
    }
    public TestData(String key, int value) {
        this.key = key;
        this.value = value;
    }
    public String getKey() {
        return key;
    }
    public int getValue() {
        return value;
    }
    public void setKey(String s) {
        key = s;
    }
    public void setValue(int v) {
        value = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestData testData = (TestData) o;
        return key.equals(testData.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
