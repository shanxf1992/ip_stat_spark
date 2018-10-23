package com.spark.utils

import java.sql.Connection
import javax.sql.DataSource

import com.mchange.v2.c3p0.ComboPooledDataSource

object JDBCUtils {

    //1 定c3p0数据库连接池
    private val dataSource = new ComboPooledDataSource()

    //2 返回数据库连接池
    def getDataSource: DataSource = {
        dataSource
    }

    //3 获取数据库连接
    def getConnection: Connection = {
        dataSource.getConnection
    }


}
