package com.daronfarina.mysqltools;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MySQLUtil
{
	private static final Logger LOG = LogManager.getLogger( MySQLUtil.class );
	
	static BasicDataSource DATA_SOURCE;

	public static Connection getConnection() throws SQLException
	{
		try
		{
			return DATA_SOURCE.getConnection();
		}
		catch ( SQLException e )
		{
			LOG.error( "Failure to acquire database connection", e );
			throw e;
		}
	}
	
	private static void log( String query, Object... args )
	{
		if ( !LOG.isInfoEnabled() )
			return;
		
		LOG.info( "Query > " + query );
		LOG.info( " Args > " + Arrays.asList( args ) );
	}
	
	public static int update( String query, Object... args ) throws SQLException
	{
		log( query, args );
		
		try ( Connection conn = getConnection() )
		{
			try ( PreparedStatement prepStatement = conn.prepareStatement( query ) )
			{
				setArguments( prepStatement, args );

				return prepStatement.executeUpdate();
			}
		}
	}

	public static int insertWithKey( String query, Object... args ) throws SQLException
	{
		log( query, args );
		
		try ( Connection conn = getConnection() )
		{
			try ( PreparedStatement prepStatement = conn.prepareStatement( query ) )
			{
				setArguments( prepStatement, args );

				prepStatement.executeUpdate();
				prepStatement.execute( "SELECT LAST_INSERT_ID()" );
				
				try ( ResultSet resultSet = prepStatement.getResultSet() )
				{
					resultSet.next();
					return resultSet.getInt( 1 );
				}
			}
		}
	}

	public static void simpleExecuteCallable( String callable ) throws SQLException
	{
		log ( callable );
		
		try ( Connection conn = getConnection() )
		{
			try ( CallableStatement statement = conn.prepareCall( callable ) )
			{
				statement.execute();
			}
		}
	}

	public static String stringResult( String query, Object... args ) throws SQLException
	{
		log( query, args );
		
		try ( Connection conn = getConnection() )
		{
			try ( PreparedStatement prepStatement = conn.prepareStatement( query ) )
			{
				setArguments( prepStatement, args );
				prepStatement.execute();
				
				try ( ResultSet resultSet = prepStatement.getResultSet() )
				{
					resultSet.next();
					return resultSet.getString( 1 );
				}
			}
		}
	}

	public static int integerResult( String query, Object... args ) throws SQLException
	{
		String stringResult = stringResult( query, args );

		if ( stringResult != null && !stringResult.isEmpty() )
		{
			try
			{
				return Integer.valueOf( stringResult );
			}
			catch ( NumberFormatException e )
			{
				throw new SQLException( "Value returned was not an integer", e );
			}
		}
		else
		{
			return 0;
		}
	}

	public static boolean simpleSingleBooleanResult( String query, Object... args ) throws SQLException
	{
		return integerResult( query, args ) == 1;
	}
	
	public static void setArguments( PreparedStatement prepStatement, Object... args ) throws SQLException
	{
		int index = 1;
		
		for ( Object arg : args )
		{
			if ( arg instanceof Integer )
			{
				prepStatement.setInt( index++, (Integer) arg );
			}
			else
			{
				prepStatement.setString( index++, Objects.toString( arg ) );
			}
		}
	}
}
