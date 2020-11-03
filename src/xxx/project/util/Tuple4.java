package xxx.project.util;

public class Tuple4<E, T, K, L> {
    private E e;
    private T t;
    private K k;
    private L l;

    public Tuple4 (E e, T t, K k, L l) {
        this.e = e;
        this.t = t;
        this.k = k;
        this.l = l;
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

    public L _4 () {
        return l;
    }

    public void set_1(E x) {
        e = x;
    }

    public void set_2(T x) {
        t = x;
    }

    public void set_3(K x) {
        k = x;
    }

    public void set_4(L x) {
        l = x;
    }
}

