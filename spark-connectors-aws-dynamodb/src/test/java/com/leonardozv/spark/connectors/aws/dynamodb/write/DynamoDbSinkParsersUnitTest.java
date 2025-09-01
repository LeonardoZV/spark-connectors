package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDbSinkParsersUnitTest {

    private static Set<Class<?>> toSet(Class<? extends Throwable>[] arr) {
        return new HashSet<>(Arrays.asList(arr));
    }

    @Test
    void constructor_whenForcedToInstantiate_throwsIllegalStateException() throws Exception {

        Constructor<DynamoDbSinkParsers> constructor = DynamoDbSinkParsers.class.getDeclaredConstructor();

        constructor.setAccessible(true);

        InvocationTargetException ite = assertThrows(InvocationTargetException.class, constructor::newInstance);

        Throwable cause = ite.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("Utility class", cause.getMessage());

    }

    @Test
    void when_nullInput_should_returnEmptyArray() {

        Class<? extends Throwable>[] out = DynamoDbSinkParsers.parseExceptions(null);

        assertNotNull(out);
        assertEquals(0, out.length);

    }

    @Test
    void when_emptySet_should_returnEmptyArray() {

        Class<? extends Throwable>[] out = DynamoDbSinkParsers.parseExceptions(Collections.emptySet());

        assertNotNull(out);
        assertEquals(0, out.length);

    }

    @Test
    void when_validExceptionClasses_areParsed_ignoringSpacesAndOrder() {

        Set<String> in = new LinkedHashSet<>(Arrays.asList(
                " java.io.IOException ",
                "java.lang.IllegalArgumentException",
                "   "
        ));

        Class<? extends Throwable>[] out = DynamoDbSinkParsers.parseExceptions(in);

        Set<Class<?>> expected = new HashSet<>(Arrays.asList(
                IOException.class,
                IllegalArgumentException.class
        ));

        assertEquals(expected, toSet(out));

        for (Class<? extends Throwable> c : out) {
            assertTrue(Throwable.class.isAssignableFrom(c));
        }

    }

    @Test
    void when_nonExceptionClass_then_throwsIllegalArgumentException() {

        Set<String> in = Set.of("java.lang.String"); // não é Throwable

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> DynamoDbSinkParsers.parseExceptions(in)
        );
        assertTrue(ex.getMessage().startsWith("Class is not an exception:"));

    }

    @Test
    void when_classNotFound_then_throwsIllegalArgumentExceptionWithCause() {

        Set<String> in = Set.of("com.exemplo.inexistente.NaoExisteException");

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> DynamoDbSinkParsers.parseExceptions(in)
        );
        assertTrue(ex.getMessage().startsWith("Class not found:"));
        assertNotNull(ex.getCause());
        assertEquals(ClassNotFoundException.class, ex.getCause().getClass());

    }

}
