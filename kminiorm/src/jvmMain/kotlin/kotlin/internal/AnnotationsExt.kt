package kotlin.internal

// https://github.com/JetBrains/kotlin/blob/1.2.50/libraries/stdlib/src/kotlin/internal/Annotations.kt
/**
 * Specifies that the corresponding type should be ignored during type inference.
 */
@Target(AnnotationTarget.TYPE)
@Retention(AnnotationRetention.BINARY)
internal annotation class NoInfer

/**
 * Specifies that the constraint built for the type during type inference should be an equality one.
 */
@Target(AnnotationTarget.TYPE)
@Retention(AnnotationRetention.BINARY)
internal annotation class Exact

/**
 * The value of this type parameter should be mentioned in input types (argument types, receiver type or expected type).
 */
@Target(AnnotationTarget.TYPE_PARAMETER)
@Retention(AnnotationRetention.BINARY)
internal annotation class OnlyInputTypes
