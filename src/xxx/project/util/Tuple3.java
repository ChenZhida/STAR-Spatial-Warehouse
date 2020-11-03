package xxx.project.util;

public class Tuple3<E, T, K> {
    private E e;
    private T t;
    private K k;

    public Tuple3 (E e, T t, K k) {
        this.e = e;
        this.t = t;
        this.k = k;
    }

    public E _1 () {
        return e;
    }

    public T _2 () {
        return t;
    }

    public K _3 () {
        return k;
    }
}
