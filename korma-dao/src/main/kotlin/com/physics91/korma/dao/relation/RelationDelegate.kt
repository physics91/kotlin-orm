package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.Entity
import com.physics91.korma.dao.entity.EntityTable
import com.physics91.korma.schema.Column
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

/**
 * Delegate for lazy loading a single related entity (ManyToOne, OneToOne).
 *
 * @param TARGET_ID The target entity's primary key type
 * @param TARGET The target entity type
 */
class SingleRelationDelegate<TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    private val targetTable: EntityTable<TARGET_ID, TARGET>,
    private val foreignKeyGetter: () -> TARGET_ID?,
    private val loader: (TARGET_ID) -> TARGET?
) : ReadOnlyProperty<Any?, TARGET?> {

    private var loaded = false
    private var cachedValue: TARGET? = null

    override fun getValue(thisRef: Any?, property: KProperty<*>): TARGET? {
        if (!loaded) {
            val foreignKey = foreignKeyGetter()
            cachedValue = if (foreignKey != null) loader(foreignKey) else null
            loaded = true
        }
        return cachedValue
    }

    /**
     * Reset the cached value to force reload on next access.
     */
    fun reset() {
        loaded = false
        cachedValue = null
    }
}

/**
 * Delegate for lazy loading a collection of related entities (OneToMany, ManyToMany).
 *
 * @param TARGET The target entity type
 */
class CollectionRelationDelegate<TARGET : Entity<*>>(
    private val loader: () -> List<TARGET>
) : ReadOnlyProperty<Any?, List<TARGET>> {

    private var loaded = false
    private var cachedValue: List<TARGET> = emptyList()

    override fun getValue(thisRef: Any?, property: KProperty<*>): List<TARGET> {
        if (!loaded) {
            cachedValue = loader()
            loaded = true
        }
        return cachedValue
    }

    /**
     * Reset the cached value to force reload on next access.
     */
    fun reset() {
        loaded = false
        cachedValue = emptyList()
    }
}

/**
 * Mutable delegate for a single related entity.
 * Allows setting the related entity directly.
 *
 * @param TARGET_ID The target entity's primary key type
 * @param TARGET The target entity type
 */
class MutableSingleRelationDelegate<TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    private val targetTable: EntityTable<TARGET_ID, TARGET>,
    private val foreignKeyGetter: () -> TARGET_ID?,
    private val foreignKeySetter: (TARGET_ID?) -> Unit,
    private val loader: (TARGET_ID) -> TARGET?
) : ReadWriteProperty<Any?, TARGET?> {

    private var loaded = false
    private var cachedValue: TARGET? = null

    override fun getValue(thisRef: Any?, property: KProperty<*>): TARGET? {
        if (!loaded) {
            val foreignKey = foreignKeyGetter()
            cachedValue = if (foreignKey != null) loader(foreignKey) else null
            loaded = true
        }
        return cachedValue
    }

    override fun setValue(thisRef: Any?, property: KProperty<*>, value: TARGET?) {
        cachedValue = value
        foreignKeySetter(value?.id)
        loaded = true
    }

    /**
     * Reset the cached value to force reload on next access.
     */
    fun reset() {
        loaded = false
        cachedValue = null
    }
}

/**
 * Mutable delegate for a collection of related entities.
 * Allows modifying the collection.
 *
 * @param TARGET The target entity type
 */
class MutableCollectionRelationDelegate<TARGET : Entity<*>>(
    private val loader: () -> List<TARGET>,
    private val onChange: (List<TARGET>) -> Unit = {}
) : ReadWriteProperty<Any?, MutableList<TARGET>> {

    private var loaded = false
    private var cachedValue: ObservableMutableList<TARGET>? = null

    override fun getValue(thisRef: Any?, property: KProperty<*>): MutableList<TARGET> {
        if (!loaded || cachedValue == null) {
            cachedValue = ObservableMutableList(loader().toMutableList(), onChange)
            loaded = true
        }
        return cachedValue!!
    }

    override fun setValue(thisRef: Any?, property: KProperty<*>, value: MutableList<TARGET>) {
        cachedValue = ObservableMutableList(value.toMutableList(), onChange)
        onChange(value)
        loaded = true
    }

    /**
     * Reset the cached value to force reload on next access.
     */
    fun reset() {
        loaded = false
        cachedValue = null
    }
}

/**
 * A MutableList that notifies on changes.
 */
class ObservableMutableList<E>(
    private val delegate: MutableList<E>,
    private val onChange: (List<E>) -> Unit
) : MutableList<E> by delegate {

    override fun add(element: E): Boolean {
        val result = delegate.add(element)
        if (result) onChange(delegate)
        return result
    }

    override fun add(index: Int, element: E) {
        delegate.add(index, element)
        onChange(delegate)
    }

    override fun addAll(elements: Collection<E>): Boolean {
        val result = delegate.addAll(elements)
        if (result) onChange(delegate)
        return result
    }

    override fun addAll(index: Int, elements: Collection<E>): Boolean {
        val result = delegate.addAll(index, elements)
        if (result) onChange(delegate)
        return result
    }

    override fun clear() {
        delegate.clear()
        onChange(delegate)
    }

    override fun remove(element: E): Boolean {
        val result = delegate.remove(element)
        if (result) onChange(delegate)
        return result
    }

    override fun removeAll(elements: Collection<E>): Boolean {
        val result = delegate.removeAll(elements)
        if (result) onChange(delegate)
        return result
    }

    override fun removeAt(index: Int): E {
        val result = delegate.removeAt(index)
        onChange(delegate)
        return result
    }

    override fun retainAll(elements: Collection<E>): Boolean {
        val result = delegate.retainAll(elements)
        if (result) onChange(delegate)
        return result
    }

    override fun set(index: Int, element: E): E {
        val result = delegate.set(index, element)
        onChange(delegate)
        return result
    }
}

/**
 * Reference holder for related entities.
 * Used when the entity itself is not loaded but we have the foreign key.
 *
 * @param ID The entity's primary key type
 * @param E The entity type
 */
class EntityReference<ID : Comparable<ID>, E : Entity<ID>>(
    val id: ID,
    private val loader: (ID) -> E?
) {
    private var loaded = false
    private var cachedEntity: E? = null

    /**
     * Load and return the referenced entity.
     */
    fun load(): E? {
        if (!loaded) {
            cachedEntity = loader(id)
            loaded = true
        }
        return cachedEntity
    }

    /**
     * Check if the entity has been loaded.
     */
    fun isLoaded(): Boolean = loaded

    /**
     * Get the cached entity without loading.
     * Returns null if not loaded.
     */
    fun getIfLoaded(): E? = if (loaded) cachedEntity else null

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityReference<*, *>) return false
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String = "EntityReference(id=$id, loaded=$loaded)"
}
