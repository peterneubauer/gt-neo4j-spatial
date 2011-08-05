/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataSourceException;
import org.geotools.data.DataStore;
import org.geotools.util.KVP;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * DataStoreFactorySpi implementation. It needs an "url" parameter containing a
 * path of a Neo4j neostore.id file.
 * 
 * @author Davide Savazzi, Andreas Wilhelm
 */
public class Neo4jSpatialDataStoreFactory extends AbstractDataStoreFactory
		implements org.geotools.data.DataStoreFactorySpi {
	
	/**
	 * url to the neostore.id file.
	 */
	public static final Param DIRECTORY = new Param("The directory path of the Neo4j database: ", File.class,
			"db", true);

	public static final Param DBTYPE = new Param("dbtype", String.class,
			"must be 'neo4j'", true, "neo4j", new KVP(Param.LEVEL, "program"));

	/**
	 * Creates a new instance of PostgisDataStoreFactory
	 */
	public Neo4jSpatialDataStoreFactory() {
	}

	public boolean canProcess(Map params) {
		if (!(((String) params.get("dbtype")).equalsIgnoreCase("neo4j"))) {
			return (false);
		} else {
			return (true);
		}
	}

	/**
	 * Construct a postgis data store using the params.
	 * 
	 * @param params
	 *            The full set of information needed to construct a live data
	 *            source. Should have dbtype equal to postgis, as well as host,
	 *            user, passwd, database, and table.
	 * 
	 * @return The created DataSource, this may be null if the required resource
	 *         was not found or if insufficent parameters were given. Note that
	 *         canProcess() should have returned false if the problem is to do
	 *         with insuficent parameters.
	 * 
	 * @throws IOException
	 *             See DataSourceException
	 * @throws DataSourceException
	 *             Thrown if there were any problems creating or connecting the
	 *             datasource.
	 */
	public DataStore createDataStore(Map params) throws IOException {

		if (!canProcess(params)) {
			throw new IOException("The parameters map isn't correct!!");
		}
		
	    File neodir = (File) DIRECTORY.lookUp(params);

		GraphDatabaseService db = new EmbeddedGraphDatabase(neodir.getAbsolutePath());
		Neo4jSpatialDataStore dataStore = new Neo4jSpatialDataStore(db);

		return dataStore;
	}

	/**
	 * Postgis cannot create a new database.
	 * 
	 * @param params
	 * 
	 * 
	 * @throws IOException
	 *             See UnsupportedOperationException
	 * @throws UnsupportedOperationException
	 *             Cannot create new database
	 */
	public DataStore createNewDataStore(Map params) throws IOException {
		throw new UnsupportedOperationException(
				"Neo4j Spatial cannot create a new database!");
	}

	public String getDisplayName() {
		return "Neo4j Spatial";
	}


	public String getDescription() {
		return "Neo4j Spatial database";
	}


	public boolean isAvailable() {
		return true;
	}

	/*
	 * @see org.geotools.data.DataStoreFactorySpi#getParametersInfo()
	 */
	public Param[] getParametersInfo() {
		return new Param[] { DBTYPE, DIRECTORY };
	}

}