package info.jerrinot.jmssink.impl;

import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.*;

public class CloningSupplier<T> implements DistributedSupplier<T> {
    private byte[] deserializedBlueprint;

    private CloningSupplier(T blueprint) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(blueprint);
            deserializedBlueprint = baos.toByteArray();
        } catch (IOException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    public static <T> CloningSupplier<T> cloneAndSupply(T blueprint) {
        return new CloningSupplier<>(blueprint);
    }

    @Override
    public T get() {
        ByteArrayInputStream bais = new ByteArrayInputStream(deserializedBlueprint);
        try {
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } catch (IOException e) {
            throw ExceptionUtil.sneakyThrow(e);
        } catch (ClassNotFoundException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
