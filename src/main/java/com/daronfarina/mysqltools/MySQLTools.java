package com.daronfarina.mysqltools;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;

public class MySQLTools
{
	private static final Logger LOG = LogManager.getLogger( MySQLTools.class );
	
	private static final String JDBC_DRIVER = "jdbc:mysql://%s/%s";
//	private static final String JDBC_DRIVER = "jdbc:mysql://%s/%s?user=%s&password=%s";

	private static String LAST_KNOWN_RESOURCE_PATH;
	
	public static void setResourcePath( String resourcePath ) throws IOException
	{
		LOG.info( "Loading properties from > " + resourcePath );

		Properties dbProps = new Properties();
		
		try ( FileReader reader = new FileReader( resourcePath ) )
		{
			dbProps.load( reader );
		}

		LAST_KNOWN_RESOURCE_PATH = resourcePath;
		
		dbProps.list( System.out );
		
		MySQLUtil.DATA_SOURCE = new BasicDataSource();
		MySQLUtil.DATA_SOURCE.setDriverClassName( "com.mysql.cj.jdbc.Driver" );
		
		String url = String.format( JDBC_DRIVER, dbProps.getProperty( "host" ), dbProps.getProperty( "schema" ) );
		MySQLUtil.DATA_SOURCE.setUrl( url );
		
		MySQLUtil.DATA_SOURCE.setUsername( dbProps.getProperty( "user" ) );
		MySQLUtil.DATA_SOURCE.setPassword( dbProps.getProperty( "pass" ) );
		MySQLUtil.DATA_SOURCE.setValidationQuery( dbProps.getProperty( "validationQuery", "SELECT 1" ) );
		MySQLUtil.DATA_SOURCE.setTestWhileIdle( true );
	}
	
	public static void refreshProperties() throws MySQLToolsException, IOException
	{
		if ( Strings.isNullOrEmpty( LAST_KNOWN_RESOURCE_PATH ) )
			throw new MySQLToolsException( "Resource Path is unknown. Call setResourcePath(String) first" );
		
		setResourcePath( LAST_KNOWN_RESOURCE_PATH );
	}
}