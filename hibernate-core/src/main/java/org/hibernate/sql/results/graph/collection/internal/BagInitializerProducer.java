/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.sql.results.graph.collection.internal;

import org.hibernate.LockMode;
import org.hibernate.collection.spi.CollectionInitializerProducer;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.collection.CollectionInitializer;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * @author Steve Ebersole
 */
public class BagInitializerProducer implements CollectionInitializerProducer {
	private final PluralAttributeMapping bagDescriptor;
	private final @Nullable Fetch collectionIdFetch;
	private final Fetch elementFetch;

	public BagInitializerProducer(
			PluralAttributeMapping bagDescriptor,
			Fetch collectionIdFetch,
			Fetch elementFetch) {
		this.bagDescriptor = bagDescriptor;

		if ( bagDescriptor.getIdentifierDescriptor() != null ) {
			assert collectionIdFetch != null;
			this.collectionIdFetch = collectionIdFetch;
		}
		else {
			assert collectionIdFetch == null;
			this.collectionIdFetch = null;
		}

		this.elementFetch = elementFetch;

	}

	@Override
	public CollectionInitializer produceInitializer(
			NavigablePath navigablePath,
			PluralAttributeMapping attributeMapping,
			FetchParentAccess parentAccess,
			LockMode lockMode,
			DomainResultAssembler<?> collectionKeyAssembler,
			DomainResultAssembler<?> collectionValueKeyAssembler,
			AssemblerCreationState creationState) {
		throw new UnsupportedOperationException( "Use the non-deprecated method variant instead." );
	}

	@Override
	public CollectionInitializer produceInitializer(
			NavigablePath navigablePath,
			PluralAttributeMapping attribute,
			InitializerParent parent,
			LockMode lockMode,
			DomainResult<?> collectionKeyResult,
			DomainResult<?> collectionValueKeyResult,
			boolean isResultInitializer,
			AssemblerCreationState creationState) {
		return new BagInitializer(
				navigablePath,
				bagDescriptor,
				parent,
				lockMode,
				collectionKeyResult,
				collectionValueKeyResult,
				isResultInitializer,
				creationState,
				elementFetch,
				collectionIdFetch
		);
	}
}
