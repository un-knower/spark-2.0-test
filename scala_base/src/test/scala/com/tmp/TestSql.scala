package com.tmp

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by yxl on 2018/3/29.
  */
object TestSql {

    def main(args: Array[String]): Unit = {

        val sql =
            """
              |insert overwrite table tmp_dev_3.tmp_gold_carteam_captain partition(p_day)
              |select
              |	s1.id,
              |	s1.name,
              |	s1.role_id,
              |	s1.created_at,
              |	s2.creater_id as create_sales_id,
              |	s2.owner_id as sales_id,
              |	s2.last_updater_id,
              |	'${yesterday}' as p_day
              |from dim_beeper.dim_beeper_gold_carteam_service_users s1
              |left join fact_beeper.fact_beeper_pre_loan_processes s2 on s1.id = s2.user_id  and s2.status != 0
              |where s1.p_day = "2018-03-28" ;
            """.stripMargin

        val calendar = Calendar.getInstance()
        calendar.add(Calendar.DAY_OF_MONTH, -210)
        var start = calendar.getTime


        println(start)

        val end = Calendar.getInstance().getTime

        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        while(start.compareTo(end) < 0){
            println(sql.replace("${yesterday}",sdf.format(start)))
            val tmp = Calendar.getInstance()
            tmp.setTime(start)
            tmp.add(Calendar.DAY_OF_MONTH,1)
            start = tmp.getTime
        }



    }
}
