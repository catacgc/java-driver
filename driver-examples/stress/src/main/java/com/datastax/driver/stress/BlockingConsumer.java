/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.stress;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingConsumer implements Consumer {

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private final Runner runner = new Runner();

    private final Session session;
    private final QueryGenerator requests;
    private final Reporter reporter;

    public BlockingConsumer(Session session,
                            QueryGenerator requests,
                            Reporter reporter) {
        this.session = session;
        this.requests = requests;
        this.reporter = reporter;
        this.runner.setDaemon(true);
    }

    @Override
    public void start() {
        this.runner.start();
    }

    @Override
    public void join() {
        Uninterruptibles.joinUninterruptibly(this.runner);
    }

    private class Runner extends Thread {



        public Runner() {
            super("Consumer Threads");
        }

        public void run() {
            try {
                while (requests.hasNext())
                    handle(requests.next());
            } catch (DriverException e) {
	            logger.debug("Error during query: " + e.getMessage());
            }
        }

        protected void handle(QueryGenerator.Request request) {
            Reporter.Context ctx = reporter.newRequest();
            try {
	            ResultSetFuture future = request.executeAsync(session);
	            try {
				    future.getUninterruptibly(Long.parseLong(System.getProperty("timeout", "20")), TimeUnit.MILLISECONDS);
	            } catch (TimeoutException e) {
		            logger.debug("Error during query: " + e.getMessage());
		            future.cancel(true);
	            }

            } finally {
                ctx.done();
            }
        }
    }
}
