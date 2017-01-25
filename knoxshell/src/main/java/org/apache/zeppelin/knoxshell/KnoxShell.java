/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.knoxshell;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.hbase.HBase;
import org.apache.hadoop.gateway.shell.hdfs.Hdfs;
import org.apache.hadoop.gateway.shell.job.Job;
import org.apache.hadoop.gateway.shell.workflow.Workflow;
import org.apache.hadoop.gateway.shell.yarn.Yarn;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Knox shell interpreter for Zeppelin.
 *
 */
public class KnoxShell extends Interpreter {

  Logger logger = LoggerFactory.getLogger(KnoxShell.class);

  private static String[] IMPORTS = new String[] {
      Hadoop.class.getName(),
      HBase.class.getName(),
      Hdfs.class.getName(),
      Job.class.getName(),
      Workflow.class.getName(),
      Yarn.class.getName(),
      TimeUnit.class.getName()
  };


  public KnoxShell(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext context) {
    Binding binding = new Binding();
    binding.setProperty("out", new PrintStream(context.out));
    GroovyShell shell = new GroovyShell(binding);
    StringBuffer imports = new StringBuffer();
    for ( String name : IMPORTS ) {
      imports.append("import " + name + ";\n");
    }
    script = imports + script;
    Object result;
    try {
      result = shell.evaluate(script);
    } catch ( Exception e ) {
      e.printStackTrace();
//      logger.debug(e.getCause().getMessage());
      result = e.getMessage();
    }
    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    if (result != null) {
      interpreterResult.add(result.toString());
    }
    return interpreterResult;
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }
}
