package com.leonardozv.spark.connectors.aws.sqs.write;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SqsSinkParsersUnitTest {

    private static Set<Class<?>> toSet(Class<? extends Throwable>[] arr) {
        return new HashSet<>(Arrays.asList(arr));
    }

    @Test
    void constructor_whenForcedToInstantiate_throwsIllegalStateException() throws Exception {

        Constructor<SqsSinkParsers> constructor = SqsSinkParsers.class.getDeclaredConstructor();

        constructor.setAccessible(true);

        InvocationTargetException ite = assertThrows(InvocationTargetException.class, constructor::newInstance);

        Throwable cause = ite.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("Utility class", cause.getMessage());

    }

    @Test
    void when_nullInput_should_returnEmptyArray() {

        Class<? extends Throwable>[] out = SqsSinkParsers.parseExceptions(null);

        assertNotNull(out);
        assertEquals(0, out.length);

    }

    @Test
    void when_emptySet_should_returnEmptyArray() {

        Class<? extends Throwable>[] out = SqsSinkParsers.parseExceptions(Collections.emptySet());

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

        Class<? extends Throwable>[] out = SqsSinkParsers.parseExceptions(in);

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
                () -> SqsSinkParsers.parseExceptions(in)
        );
        assertTrue(ex.getMessage().startsWith("Class is not an exception:"));

    }

    @Test
    void when_classNotFound_then_throwsIllegalArgumentExceptionWithCause() {

        Set<String> in = Set.of("com.exemplo.inexistente.NaoExisteException");

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SqsSinkParsers.parseExceptions(in)
        );
        assertTrue(ex.getMessage().startsWith("Class not found:"));
        assertNotNull(ex.getCause());
        assertEquals(ClassNotFoundException.class, ex.getCause().getClass());

    }

}
