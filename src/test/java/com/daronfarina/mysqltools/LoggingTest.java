package com.daronfarina.mysqltools;

import java.io.File;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class LoggingTest extends TestCase
{
	private static final Logger LOG = LogManager.getLogger( LoggingTest.class );

	private static final String LOG4J_PROPS = "src/test/resources/log4j.properties";

	public LoggingTest( String testName )
	{
		super( testName );
	}

	public static Test suite()
	{
		return new TestSuite( LoggingTest.class );
	}

	public void testLogging()
	{
		PropertyConfigurator.configure( new File( LOG4J_PROPS ).getAbsolutePath() );
		LOG.fatal( "" );
		LOG.error( "" );
		LOG.warn( "" );
		LOG.info( "" );
		LOG.debug( "" );
	}

	public void testExceptioHandling()
	{
		try
		{
			Integer.parseInt( "not an int" );
		}
		catch ( NumberFormatException e )
		{
			LOG.error( "Planned message", e );
		}
	}
}