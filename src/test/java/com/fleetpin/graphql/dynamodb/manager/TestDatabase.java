package com.fleetpin.graphql.dynamodb.manager;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@ArgumentsSource(TestDatabaseProvider.class)
public @interface TestDatabase {
    boolean useProd() default false;

    String[] organisationIds() default {"test"};
}
