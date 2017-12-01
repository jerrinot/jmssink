package info.jerrinot.o2.o2camel;

import java.io.Serializable;

public final class Pair<L, R> implements Serializable {
    private final L left;
    private final R right;

    private Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public static <LEFT> Builder<LEFT> of(LEFT left) {
        return new Builder<>(left);
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    public static final class Builder<LEFT> {
        private LEFT left;

        public Builder(LEFT left) {
            this.left = left;
        }

        public <RIGHT> Pair<LEFT, RIGHT> and(RIGHT right) {
            return new Pair<>(left, right);
        }
    }
}
