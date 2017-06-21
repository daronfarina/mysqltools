package com.daronfarina.mysqltools;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MySQLWorker
{
	private static final Logger LOG = LogManager.getLogger( MySQLWorker.class );
	
	private final Connection connection;
	private PreparedStatement statement;
	private ResultSet resultSet;
	private boolean closeConnectionOnError;

	public MySQLWorker() throws SQLException
	{
		connection = MySQLUtil.getConnection();
		statement = null;
		resultSet = null;
		closeConnectionOnError = true;
	}

	public MySQLWorker( String query, String... args ) throws SQLException
	{
		connection = MySQLUtil.getConnection();
		execute( query, args );
	}

	public MySQLWorker( String query, InputStream blob ) throws SQLException
	{
		connection = MySQLUtil.getConnection();
		execute( query, blob );
	}

	public MySQLWorker( StringBuilder query, String... args ) throws SQLException
	{
		this( query.toString(), args );
	}

	public MySQLWorker( StringBuilder query, InputStream blob ) throws SQLException
	{
		this( query.toString(), blob );
	}

	public boolean isCloseConnectionOnError()
	{
		return closeConnectionOnError;
	}

	/**
	 * This was added to support being able to <code>ROLLBACK</code> after an
	 * error occurs during a <code>TRANSACTION</code>. The default for this
	 * value is <code>true</code>. Setting this value to <code>false</code> will
	 * allow statements to be executed after an error is thrown.
	 */
	public void setCloseConnectionOnError( boolean closeOnError )
	{
		this.closeConnectionOnError = closeOnError;
	}

	public ResultSet execute( String query, String... args ) throws SQLException
	{
		if ( resultSet != null )
			resultSet.close();

		if ( statement != null )
			statement.close();

		statement = connection.prepareStatement( query );

		for ( int i = 0; i < args.length; i++ )
			statement.setString( i + 1, args[i] );

		statement.execute();
		resultSet = statement.getResultSet();
		return resultSet;
	}

	public ResultSet execute( String query, InputStream blob ) throws SQLException
	{
		statement = connection.prepareStatement( query );
		statement.setBlob( 1, blob );

		statement.execute();
		resultSet = statement.getResultSet();
		return resultSet;
	}

	public ResultSet execute( StringBuilder query, String... args ) throws SQLException
	{
		return execute( query.toString(), args );
	}

	public ResultSet execute( StringBuilder query, InputStream blob ) throws SQLException
	{
		return execute( query.toString(), blob );
	}

	public ResultSetMetaData getResultSetMetaData() throws SQLException
	{
		return resultSet.getMetaData();
	}

	public String getExecutedQuery()
	{
		if ( statement == null )
			return "";

		return statement.toString();
	}

	public int getCurrentRow() throws SQLException
	{
		return resultSet.getRow();
	}

	public int getNumberOfColumns() throws SQLException
	{
		return getResultSetMetaData().getColumnCount();
	}

	public String getColumnName( int columnNumber ) throws SQLException
	{
		return getResultSetMetaData().getColumnName( columnNumber );
	}

	public ResultSet getResultSet()
	{
		return resultSet;
	}

	public void close()
	{
		try
		{
			if ( connection != null )
				connection.close();

			if ( statement != null )
				statement.close();

			if ( resultSet != null )
				resultSet.close();
		}
		catch ( SQLException e )
		{
			LOG.error( "Failure to close MySQL resources", e );
		}
	}

	public String getQuery()
	{
		if ( statement == null )
			return "STATEMENT NOT INITIALIZED";

		return statement.toString();
	}

	// ========================
	// Gets - Wrapped ResultSet
	// ========================

	public int getNumberOfRows() throws SQLException
	{
		if ( resultSet == null )
			return 0;

		int rows = 0;

		resultSet.last();
		rows = resultSet.getRow();
		resultSet.beforeFirst();

		return rows;
	}

	public boolean next() throws SQLException
	{
		return resultSet.next();
	}

	public String getString( String columnName ) throws SQLException
	{
		return resultSet.getString( columnName );
	}

	public String getString( int columnNumber ) throws SQLException
	{
		return resultSet.getString( columnNumber );
	}

	public int getInt( String columnName ) throws SQLException
	{
		return resultSet.getInt( columnName );
	}

	public int getInt( int columnNumber ) throws SQLException
	{
		return resultSet.getInt( columnNumber );
	}

	public boolean getBoolean( String columnName ) throws SQLException
	{
		return resultSet.getBoolean( columnName );
	}

	public boolean getBoolean( int columnNumber ) throws SQLException
	{
		return resultSet.getBoolean( columnNumber );
	}

	public Date getDate( String columnName ) throws SQLException
	{
		return resultSet.getDate( columnName );
	}

	public Date getDate( int columnNumber ) throws SQLException
	{
		return resultSet.getDate( columnNumber );
	}

	public Date getTimestamp( String columnName ) throws SQLException
	{
		return resultSet.getTimestamp( columnName );
	}

	public Date getTimestamp( int columnNumber ) throws SQLException
	{
		return resultSet.getTimestamp( columnNumber );
	}

	public double getDouble( String columnName ) throws SQLException
	{
		return resultSet.getDouble( columnName );
	}

	public double getDouble( int columnNumber ) throws SQLException
	{
		return resultSet.getDouble( columnNumber );
	}

	public InputStream getBlobAsInputStream( String columnName ) throws SQLException
	{
		return resultSet.getBlob( columnName ).getBinaryStream();
	}

	public InputStream getBlobAsInputStream( int columnNumber ) throws SQLException
	{
		return resultSet.getBlob( columnNumber ).getBinaryStream();
	}

	public Object getObject( String columnName ) throws SQLException
	{
		return resultSet.getObject( columnName );
	}

	public Object getObject( int columnNumber ) throws SQLException
	{
		return resultSet.getObject( columnNumber );
	}

	public void beforeFirst() throws SQLException
	{
		resultSet.beforeFirst();
	}
}