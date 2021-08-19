/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * @author Clinton Begin
 */
public class SimpleExecutor extends BaseExecutor {

  public SimpleExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      /**
       * 这里创建一个SatementHandler，这个Handler是 RoutingStatementHandler 其内部的deletegate是PreparedStatementHandler
       */
      StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
      /**
       * 这里执行prepareStatement，在改方法内首先getConnection，这个实现实际是委托给了SpringManagedTransaction的getConnection
       * 而且如果开启了debug级别 对是SpringManagedTransaction中获取到的Connection还进行代理，进行代理时使用ConnectionLogger作为InvocationHandler，使用Connection接口作为被代理的接口。
       *最终我们得到的connection是一个代理对象。 因此执行Connection接口中的方法就会执行 InvocationHandler（ConnectionLogger）中的invoke方法
       *  Connection connection = transaction.getConnection();
       *     if (statementLog.isDebugEnabled()) {
       *       //实现对Connection对象的代理
       *       return ConnectionLogger.newInstance(connection, statementLog, queryStack);
       *     } else {
       *       return connection;
       *     }
       *
       * 我们以执行connection的prepareStatement方法为例， 在ConnectionLogger中，如果判断当前执行的Connection 的方法是prepareStatement，则会执行如下逻辑
       *   step1:首先在目标原始connection对象上执行 method，获取到java sql包中提供的PreparedStatement对象，如果你的项目中使用了druid连接池，最终会返回一个阿里巴巴的PreparedStatementProxyImpl
       *  PreparedStatement stmt = (PreparedStatement) method.invoke(connection, params);
       *  step2,当拿到PreparedStatement 之后我们会为这个PreparedStatement 在创建一个代理对象，使用mybatis的PreparedStatementLogger 作为InvocationHandler，
       *  因此最终的效果就是执行PreparedStatement接口中的方法的时候会 执行Mybatis的PreparedStatementLogger中的invoke方法
       *  stmt = PreparedStatementLogger.newInstance(stmt, statementLog, queryStack);//创建代理对象
       *
       *
       *
       *
       *
       */
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.update(stmt);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      /**
       * 这里使用了Configuration.newStatementHandler,类似于org.apache.ibatis.session.defaults.DefaultSqlSessionFactory#openSessionFromDataSource(org.apache.ibatis.session.ExecutorType, org.apache.ibatis.session.TransactionIsolationLevel, boolean)
       * 方法中创建Executor final Executor executor = configuration.newExecutor(tx, execType);
       * 在Configuration的newExecutor中 会首先根据execType创建一个Executor，然后针对这个Executor创建代理对象，方式就是使用  executor = (Executor) interceptorChain.pluginAll(executor);
       *
       * 在这里使用了同样的方式 创建StatementHandler，在newStatementHandler内部也是首先创建一个StatementHandler，然后创建代理对象
       *  StatementHandler statementHandler = new RoutingStatementHandler(executor, mappedStatement, parameterObject, rowBounds, resultHandler, boundSql);
       *  statementHandler = (StatementHandler) interceptorChain.pluginAll(statementHandler);
       *  最终得到的是RoutingStatementHandler的代理对象
       *
       *  而且需要注意的是 RoutingStatementHandler 继承自BaseStatementHandler，在创建RoutingStatementHandler的时候必然执行BaseStatementHandler的构造器
       *  在BaseStatementHandler的构造器内执行了
       *
       *this.parameterHandler = configuration.newParameterHandler(mappedStatement, parameterObject, boundSql);
       *this.resultSetHandler = configuration.newResultSetHandler(executor, mappedStatement, rowBounds, parameterHandler, resultHandler, boundSql);
       *
       * 其中Configuration的newParameterHandler方法内 也是类似的实现
       *  ParameterHandler parameterHandler = mappedStatement.getLang().createParameterHandler(mappedStatement, parameterObject, boundSql);
       *  parameterHandler = (ParameterHandler) interceptorChain.pluginAll(parameterHandler);
       *
       * Configuration的newResultSetHandler：
       *  ResultSetHandler resultSetHandler = new DefaultResultSetHandler(executor, mappedStatement, parameterHandler, resultHandler, boundSql, rowBounds);
       *  resultSetHandler = (ResultSetHandler) interceptorChain.pluginAll(resultSetHandler);
       *
       */
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.query(stmt, resultHandler);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    Cursor<E> cursor = handler.queryCursor(stmt);
    stmt.closeOnCompletion();
    return cursor;
  }

  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) {
    return Collections.emptyList();
  }

  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
    Statement stmt;
    /**
     * 获取Connection，如果开启了debug log，这里将会返回Connection对象的代理对象，使用了ConnectionLogger作为代理对象
     */
    Connection connection = getConnection(statementLog);
    stmt = handler.prepare(connection, transaction.getTimeout());
    handler.parameterize(stmt);
    return stmt;
  }

}
