package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class ArbitraryQuery(val constQuery: ConstQuery = null,
                     val aggFunc: QueryAggFunc = null,
                     val minLength: Int = 0) extends PathQuery {}
