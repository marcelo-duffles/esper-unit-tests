package com.espertech.esper.regression.view;

import com.espertech.esper.client.*;
import com.espertech.esper.support.util.SupportUpdateListener;
import com.espertech.esper.support.bean.SupportBean;
import com.espertech.esper.support.client.SupportConfigFactory;
import junit.framework.*;

public class TestOutputFirstHaving extends TestCase {

    private EPServiceProvider epService;
    private SupportUpdateListener listener;

    public void setUp()
    {
        Configuration config = SupportConfigFactory.getConfiguration();
	config.getEngineDefaults().getLogging().setEnableExecutionDebug(true);
	config.getEngineDefaults().getLogging().setEnableTimerDebug(false);
	epService = EPServiceProviderManager.getDefaultProvider();
        epService.initialize();
        epService.getEPAdministrator().getConfiguration().addEventType("SupportBean", SupportBean.class);
	listener = new SupportUpdateListener();
    }

    // This test is similar to those found at TestOutputLimitEventPerRow.testOutputFirstHavingJoinNoJoin
    public void testHavingAvgOutputFirst()
    {
	String query = "select doublePrimitive, avg(doublePrimitive) from SupportBean having doublePrimitive > 2*avg(doublePrimitive) output first every 2 events";
	EPStatement statement = epService.getEPAdministrator().createEPL(query);
	statement.addListener(listener);

	sendBeanEvent(1);
        assertFalse("Listener not invoked", listener.isInvoked()); // doublePrimitive is 1 and avg(doublePrimitive) is 1, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' does not hold

	sendBeanEvent(2);
        assertFalse("Listener incorrectly invoked", listener.isInvoked()); // doublePrimitive is 2 and avg(doublePrimitive) is 1.5, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' does not hold

	sendBeanEvent(9);
        assertTrue("Listener not invoked, but should be", listener.isInvoked()); // doublePrimitive is 9 and avg(doublePrimitive) is 4, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' holds
     }

    // This test is similar to the previous test, but with no 'avg' in the 'having' clause condition
    public void testHavingNoAvgOutputFirst()
    {
	String query = "select doublePrimitive from SupportBean having doublePrimitive > 1 output first every 2 events";
	EPStatement statement = epService.getEPAdministrator().createEPL(query);
	statement.addListener(listener);

	sendBeanEvent(1);
        assertFalse("Listener incorrectly invoked", listener.isInvoked()); // doublePrimitive is 1, so the condition 'doublePrimitive > 1' does not hold

	sendBeanEvent(2);
        assertTrue("Listener not invoked, but should be", listener.isInvoked()); // doublePrimitive is 2, so the condition 'doublePrimitive > 1' should hold!

	sendBeanEvent(9);
        assertFalse("Listener incorrectly invoked", listener.isInvoked()); // the condition 'doublePrimitive > 1' holds, but this event should be ignored due to the expression 'every 2 events'
     }

    // This test is similar to the test testHavingAvgOutputFirst(), but with 'every 2 minutes' in the place of 'every 2 events'
    public void testHavingAvgOutputFirstEveryTwoMinutes()
    {
	String query = "select doublePrimitive, avg(doublePrimitive) from SupportBean having doublePrimitive > 2*avg(doublePrimitive) output first every 2 minutes";
	EPStatement statement = epService.getEPAdministrator().createEPL(query);
	statement.addListener(listener);

	sendBeanEvent(1);
        assertFalse("Listener incorrectly invoked", listener.isInvoked()); // doublePrimitive is 1 and avg(doublePrimitive) is 1, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' does not hold

	sendBeanEvent(2);
        assertFalse("Listener incorrectly invoked", listener.isInvoked()); // doublePrimitive is 2 and avg(doublePrimitive) is 1.5, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' does not hold

	sendBeanEvent(9);
        assertTrue("Listener not invoked, but should be", listener.isInvoked()); // doublePrimitive is 9 and avg(doublePrimitive) is 4, so the condition 'doublePrimitive > 2*avg(doublePrimitive)' should hold!

     }

     private void sendBeanEvent (double doublePrimitive)
     {
        SupportBean b = new SupportBean();
        b.setDoublePrimitive(doublePrimitive);
	epService.getEPRuntime().sendEvent(b);
     }
}

