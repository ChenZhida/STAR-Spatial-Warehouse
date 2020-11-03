package xxx.project.util;

import java.io.Serializable;

public class Tuple2<E, T> implements Serializable {
    public static final long serialVersionUID = 100L;

    private E e;
    private T t;

    public Tuple2 (E e, T t) {
        this.e = e;
        this.t = t;
    }

    public E _1 () {
        return e;
    }

    public T _2 () {
        return t;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (e != null ? !e.equals(tuple2.e) : tuple2.e != null) return false;
        return t != null ? t.equals(tuple2.t) : tuple2.t == null;
    }

    @Override
    public int hashCode() {
        int result = e != null ? e.hashCode() : 0;
        result = 31 * result + (t != null ? t.hashCode() : 0);
        return result;
    }
}

