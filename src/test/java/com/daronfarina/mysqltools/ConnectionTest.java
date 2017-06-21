package com.daronfarina.mysqltools;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ConnectionTest extends TestCase
{
	private static final String DATABASE_PROPS = "src/test/resources/db.properties";

	public ConnectionTest( String testName )
	{
		super( testName );
	}

	public static Test suite()
	{
		return new TestSuite( ConnectionTest.class );
	}

	public void testConnection()
	{
		try
		{
			MySQLTools.setResourcePath( new File( DATABASE_PROPS ).getAbsolutePath() );
		}
		catch ( IOException e )
		{
			e.printStackTrace();
			assert ( false );
		}
	}

	public void testValidation()
	{
		try
		{
			MySQLTools.setResourcePath( new File( DATABASE_PROPS ).getAbsolutePath() );
			System.out.println( ">>> Result > " + MySQLUtil.stringResult( "SELECT 1" ) );
		}
		catch ( SQLException | IOException e )
		{
			e.printStackTrace();
			assert ( false );
		}
	}

	public void testArgument()
	{
		try
		{
			MySQLTools.setResourcePath( new File( DATABASE_PROPS ).getAbsolutePath() );
			
			String username = MySQLUtil.stringResult( "SELECT CURRENT_USER()" );

			String[] usernameParts = username.split( "@" );
			
			String usernameWithHost = "'" + usernameParts[0] + "'@'" + usernameParts[1] + "'";
			
			StringBuilder query = new StringBuilder();

			query.append( "SELECT privilege_type, is_grantable FROM INFORMATION_SCHEMA.USER_PRIVILEGES " );
			query.append( "WHERE grantee = ?" );

			MySQLWorker db = new MySQLWorker( query.toString(), usernameWithHost );

			while ( db.next() )
			{
				String privilege = db.getString( "privilege_type" );
				String grantable = db.getString( "is_grantable" );
				
				System.out.println( ">>> " + privilege + " > Grantable? " + grantable );
			}
			
			db.close();
		}
		catch ( SQLException | IOException e )
		{
			e.printStackTrace();
			assert ( false );
		}
	}
}