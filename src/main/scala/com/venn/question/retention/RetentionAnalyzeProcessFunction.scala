package com.venn.question.retention

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import com.venn.util.DateTimeUtil
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * user day retention analyze process function
 */
class RetentionAnalyzeProcessFunction extends ProcessWindowFunction[UserLog, String, String, TimeWindow] {

  val LOG = LoggerFactory.getLogger("RetentionAnalyzeProcessFunction")
  val SQL = "select user_id,if(login_day = date_format(date_sub(now(), INTERVAL 1 DAY), '%Y-%m-%d'), 0, 1) last_new_user from user_info"
  var connect: Connection = _
  var ps: PreparedStatement = _
  var mysqlUrl = "jdbc:mysql://localhost:3306/venn?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
  var mysqlUser = "root"
  var mysqlPass = "123456"
  var allUserMap = new util.HashMap[String, Int]()
  var lastUserMap = new util.HashMap[String, Int]()
  val currentUser = new util.HashMap[String, Int]()

  var allUserState: ValueState[util.HashMap[String, Int]] = _
  var lastUserState: ValueState[util.HashMap[String, Int]] = _
  var currentUserState: ValueState[util.HashMap[String, Int]] = _


  /**
   * open: load all user and last day new user
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    LOG.info("RetentionAnalyzeProcessFunction open")
    // create state
    allUserState = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[String, Int]]("allUser", classOf[util.HashMap[String, Int]]))
    lastUserState = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[String, Int]]("lastUser", classOf[util.HashMap[String, Int]]))
    currentUserState = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[String, Int]]("currentUser", classOf[util.HashMap[String, Int]]))

    reconnect()
    loadUser()
  }


  /**
   * 1. find current day new user
   * 2. load last day new user
   *
   * @param key
   * @param context
   * @param elements
   * @param out
   */
  override def process(key: String, context: Context, elements: Iterable[UserLog], out: Collector[String]): Unit = {
    LOG.debug("trigger process")
    if (allUserState.value() == null) {
      allUserState.update(allUserMap)
      lastUserState.update(lastUserMap)
    } else {
      allUserMap = allUserState.value()
      lastUserMap = lastUserState.value()
    }


    val it = elements.iterator

    var lastUserLog = 0d
    while (it.hasNext) {
      val userLog = it.next()
      if (lastUserMap.containsKey(userLog.userId)) {
        lastUserLog += 1l
      }
      if (!allUserMap.containsKey(userLog.userId)) {
        currentUser.put(userLog.userId, 1)
      }
    }

    val day = DateTimeUtil.formatMillis(context.window.getStart, DateTimeUtil.YYYY_MM_DD)

    var str: String = null
    if (lastUserMap.isEmpty) {
      str = day + ",current," + currentUser.size()
    } else {
      str = day + ",current," + currentUser.size() + "," + lastUserLog / lastUserMap.size()
    }
    out.collect(str)

  }

  /**
   * export current user to all user table
   */
  def exportCurrentUser(window: TimeWindow): Unit = {
    val sql = "insert into user_info(user_id, login_day) values(?, ?)"
    val ps = connect.prepareStatement(sql)
    val set = currentUser.keySet().iterator()

    val day = DateTimeUtil.formatMillis(window.getStart, DateTimeUtil.YYYY_MM_DD)
    var count = 0
    while (set.hasNext) {
      ps.setString(1, set.next())
      ps.setString(2, day)

      ps.addBatch()
      count += 1
      if (count % 10 == 0) {
        LOG.debug("add batch")
        ps.executeBatch()
      }
    }
    if (count % 10 != 0) {
      LOG.debug("add last batch")
      ps.executeBatch()
    }
  }

  /**
   * output current day new user
   *
   * @param context
   */
  override def clear(context: Context): Unit = {
    val window = context.window
    LOG.info(String.format("window start : %s, end: %s, clear", DateTimeUtil.formatMillis(window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS), DateTimeUtil.formatMillis(window.getEnd, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)))
    // clear last user, add current user as last/all user map
    lastUserMap.clear()
    lastUserMap.putAll(currentUser)
    allUserMap.putAll(currentUser)
    lastUserState.update(lastUserMap)
    allUserState.update(allUserMap)
    // export current user to mysql userInfo
    exportCurrentUser(window)
    // clear current user
    currentUser.clear()
  }

  def reconnect() = {

    LOG.info("reconnect mysql")
    if (connect != null) {
      connect.close()
    }
    connect = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPass)
    ps = connect.prepareStatement(SQL)
  }

  def loadUser(): Unit = {

    val resultSet = ps.executeQuery()
    if (resultSet == null) {
      return
    }
    while (resultSet.next()) {
      val userId = resultSet.getString(1)
      val isLast = resultSet.getInt(2)

      if (isLast == 0) {
        lastUserMap.put(userId, isLast)
      }
      allUserMap.put(userId, isLast)
    }

    LOG.info("load all user count : " + allUserMap.size() + ", last user : " + lastUserMap)

  }
}
